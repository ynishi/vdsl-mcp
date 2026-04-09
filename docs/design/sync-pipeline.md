# Sync Pipeline Design

Accurate modeling based on code reading. No speculation.

## 1. Pipeline Sequence

```
MCP: vdsl_sync
  |
  +- SyncTaskManager::spawn_sync(sdk)
  |   +- tokio::spawn -> SdkImpl::sync()
  |
  v
Phase 0a: Ensure
  |  Location.ensure() for all locations — reachability check
  |  Failed locations -> skip_locations (sync continues)
  |
Phase 0b: Orphan InFlight Termination
  |  cancel_orphaned_inflight() — InFlight from prior crash -> Failed
  |
Phase 1: Scan -> TopologyDelta[]
  |
  |  TopologyScanner::scan_all()
  |  +- (1) LocationScanner[].scan() -> ScannedFile[]
  |  |   +- local: fs walk
  |  |   +- cloud: rclone lsjson
  |  |   +- pod: ssh find -L + batch_inspect
  |  |
  |  +- (2) compute_topology_deltas(ScannedFile[])
  |      +- DB: list_active() -> all TopologyFiles
  |      +- Group ScannedFiles by origin
  |      |
  |      +- For each ScannedFile, match_and_classify():
  |      |   +- Pass1: ByPath -> exact path match against TF
  |      |   |   +- fingerprint changed -> ContentChanged
  |      |   |   +- fingerprint same -> Skip (no delta)
  |      |   +- Pass2: ByHash -> canonical_digest match (only if Pass1 miss)
  |      |   |   +- -> Renamed
  |      |   +- No match -> Discovered
  |      |
  |      +- Vanished detection:
  |          DB LocationFiles (Active) with no matching
  |          ScannedFile (same origin, same path) -> Vanished
  |
Phase 2: Plan — TopologyStore::sync(deltas)
  |
  |  (2a) apply_ingest(deltas)
  |  |  Sort order: Renamed(0) -> ContentChanged(1) -> Discovered(2) -> Vanished(3)
  |  |
  |  |  Discovered:
  |  |    get_by_path -> reuse existing TF or create new TF
  |  |    promote_canonical_digest -> update TF.canonical_hash
  |  |    upsert TF
  |  |    materialize -> create LF (origin, path, fingerprint)
  |  |    upsert LF
  |  |
  |  |  ContentChanged:
  |  |    TF: promote_canonical_digest
  |  |    LF: update fingerprint or create new
  |  |    Other locations' LFs -> stale_candidates -> mark_stale
  |  |
  |  |  Renamed:
  |  |    TF: update_path(new_path) + promote_canonical_digest
  |  |    LF: update fingerprint or create new
  |  |
  |  |  Vanished:
  |  |    LF: mark_missing
  |  |    * TF is NOT deleted (scan-based delete propagation was reverted)
  |  |    * Deletion only via explicit delete() API
  |  |
  |  |  Returns: ingest_origins = { file_id -> {origin LocationId} }
  |  |
  |  (2b) distribute_actions(active_tfs, lf_map, locations, ingest_origins)
  |  |  For all active TFs x all Locations:
  |  |    conflict detection -> source selection -> per-target action
  |  |    - target has no LF -> NeedsCopy
  |  |    - target has LF but Stale -> NeedsCopy (Update)
  |  |    - target has LF Active + fingerprint match -> Skip
  |  |
  |  (2c) Delete transfer generation for deleted TFs
  |  |  list_deleted() -> deleted TFs
  |  |  For each deleted TF's LFs -> create Delete Transfer per dest
  |  |  (skip dest if pending delete transfer already exists)
  |  |
  |  (2d) plan_distribution -> PlannedTransfer[] -> create_transfers -> DB write
  |
Phase 3: Execute — execute_bfs()
  |
  |  BFS order, per target:
  |    queued_transfers(target) -> Transfer[]
  |    partition: sync / delete
  |
  |    Phase A: sync transfers
  |      prepare: TF lookup -> relative_path resolution
  |      engine.execute_prepared(sync_prepared)
  |        +- batch push (rclone copy --files-from)
  |        +- per-file for non-batch
  |      persist_outcomes:
  |        completed -> unblock_dependents + create LF(dest)
  |
  |    Phase B: delete transfers
  |      engine.execute_prepared(delete_prepared)
  |        +- archive_root set -> per-file archive_move (rclone moveto) *slow*
  |        +- archive_root unset -> batch delete (rclone delete --files-from)
  |      persist_outcomes:
  |        completed -> delete LF(dest)
  |
  |  Repeat BFS up to max_passes (chain transfer unblock wait)
  |
  +- Return SyncReport
```

## 2. Entity State Machines

### TopologyFile

```
                    +---------+
    new() --------->| Active  |<---- unmark_deleted() [restore]
                    |(deleted |
                    | _at=NULL|
                    +----+----+
                         | mark_deleted() [delete() API]
                         v
                    +---------+
                    | Deleted |  deleted_at = timestamp
                    |         |  -> list_deleted() retrieves these
                    |         |  -> delete transfers generated during sync
                    +---------+
```

**Note**: Vanished (not found in scan) only triggers LF.mark_missing. TF is NOT deleted.
TF deletion (mark_deleted) is only via explicit `delete()` API call.

### LocationFile

```
    materialize()
         |
         v
    +---------+  mark_stale()  +---------+
    | Active  |--------------->|  Stale  |
    |         |<---------------|         |
    +----+----+  (re-sync)     +---------+
         |
         | mark_missing()
         v
    +---------+
    | Missing |  (not found in scan)
    +---------+

    archive()
         |
         v
    +----------+
    | Archived |  (excluded from transfers)
    +----------+

    mark_syncing()
         |
         v
    +----------+
    | Syncing  |  (transfer in progress)
    +----------+
```

### Transfer

```
    new()
      |
      v
  +--------+  execute  +----------+  success  +-----------+
  | Queued |---------->| InFlight |---------->| Completed |
  +--------+           +----------+           +-----------+
      |                      |
      | (blocked)            | failure
      v                      v
  +---------+          +--------+
  | Blocked |          | Failed |
  +---------+          +--------+
```

## 3. Known Issues

### P1: Per-file archive delete is extremely slow

**Location**: `route.rs:456-466` `delete_batch()`

```rust
if self.archive_root.is_some() {
    // Archive mode: fall back to per-file archive_move.
    for rel in relative_paths {
        results.insert(rel.clone(), self.delete(rel).await);
    }
    return results;
}
```

Comment says "deletes are low-volume", but this assumption breaks during bulk operations
(e.g., clean DB rebuild). 3,514 files x rclone moveto = hours.

### P2: Delete transfers generated from stale deleted TFs

Phase 2c calls `list_deleted()`. On a clean DB this returns 0. However, if old deleted TFs
persist in the DB across sync runs, delete transfers are re-generated each time for destinations
that haven't completed the delete yet.

### P3: SDK not rebuilt when DB is externally deleted (FIXED)

When the DB file was deleted and recreated, `resolve_or_init_sdk()` returned the cached SDK
with stale store references because only pod_id was checked for invalidation.

**Fix**: Added `generation` counter to `SyncDb`. Each `ensure()` that opens a new store bumps
the generation. `resolve_or_init_sdk()` now checks both pod_id AND generation before returning
a cached SDK. Verified: clean-DB sync produced 0 delete transfers (previously 3,514+).

See commit: `fix(sync): invalidate cached SDK when SyncDb is rebuilt`

### P4: Phase display becomes stale during long operations

`report_progress()` is called at the start of `process_target_batch()` and not updated during
execution. During 3,514 per-file moveto operations, the phase string remains unchanged.
No progress visibility.

## 4. Data Flow

```
ScannedFile[] ------+
                    v
            match_and_classify()
                    |
    +---------------+---------------+--------------+
    v               v               v              v
Discovered    ContentChanged    Renamed        Vanished
    |               |               |              |
    v               v               v              v
 TF new/reuse    TF update       TF path       LF mark_missing
 LF create       LF update       update        (TF untouched)
                 other LF stale   LF update
    |               |               |
    +-------+-------+---------------+
            v
    ingest_origins: { file_id -> {origin} }
            |
            v
    distribute_actions(active_tfs, lf_map, locations, ingest_origins)
            |
            v
    DistributeAction[] --> plan_distribution() --> PlannedTransfer[]
            |                                              |
            v                                              v
    deleted TFs --> Delete Transfer[]              create_transfers()
            |                                              |
            +------------------+---------------------------+
                               v
                        Transfer[] in DB
                               |
                               v
                        execute_bfs()
                        +- sync: rclone copy (batch)
                        +- delete: rclone moveto (per-file) *P1*
```

## 5. Route Topology

```
Location Graph:
  local <--(Pull)--> cloud    archive_root = vdsl/archived
  local --(Push)---> pod      archive_root = none
  pod   --(Push)---> cloud    archive_root = vdsl/archived

Delete behavior:
  cloud dest:  archive_move (rclone moveto -> archived/)  *slow (per-file)* [P1]
  pod dest:    hard delete (rclone delete batch)           fast
  local dest:  fs::remove_file                             fast
```

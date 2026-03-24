//! RouteGraph — directed graph of location-to-location transfer topology.
//!
//! Pure domain value object. Knows only which locations can reach which others.
//! Transfer mechanics (backends, shells) are not its concern.

use std::collections::{HashMap, HashSet, VecDeque};

use super::location::LocationId;

/// Directed graph of transfer reachability between locations.
///
/// Each edge `(src, dest)` means "a file present at `src` can be transferred
/// to `dest`". The graph enables reachability queries: given an origin, which
/// destinations can eventually receive the file (including multi-hop)?
///
/// # Invariants
///
/// - Self-loops are silently rejected (`add` ignores `src == dest`).
/// - Duplicate edges are deduplicated by the underlying `HashSet`.
///
/// # Data structure
///
/// Adjacency list (`HashMap<src, HashSet<dest>>`) — O(1) lookup for
/// `has`/`remove`/`direct_from` without cloning LocationId.
#[derive(Debug, Clone, Default)]
pub struct RouteGraph {
    adj: HashMap<LocationId, HashSet<LocationId>>,
}

impl RouteGraph {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a directed edge. Self-loops (`src == dest`) are silently ignored.
    pub fn add(&mut self, src: LocationId, dest: LocationId) {
        if src != dest {
            self.adj.entry(src).or_default().insert(dest);
        }
    }

    /// Remove a directed edge. No-op if the edge does not exist.
    pub fn remove(&mut self, src: &LocationId, dest: &LocationId) {
        if let Some(dests) = self.adj.get_mut(src) {
            dests.remove(dest);
            if dests.is_empty() {
                self.adj.remove(src);
            }
        }
    }

    /// Whether a direct edge exists from `src` to `dest`.
    pub fn has(&self, src: &LocationId, dest: &LocationId) -> bool {
        self.adj.get(src).is_some_and(|dests| dests.contains(dest))
    }

    /// Direct neighbors reachable from `origin` (1-hop).
    ///
    /// 空の場合は空スライスのイテレータを返す。clone不要。
    pub fn direct_from(&self, origin: &LocationId) -> impl Iterator<Item = &LocationId> {
        self.adj.get(origin).into_iter().flat_map(|s| s.iter())
    }

    /// All locations reachable from `origin` via BFS (multi-hop).
    ///
    /// The result does **not** include `origin` itself, even if there is a
    /// cycle back to it.
    pub fn reachable_from(&self, origin: &LocationId) -> HashSet<LocationId> {
        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();

        for d in self.direct_from(origin) {
            if visited.insert(d.clone()) {
                queue.push_back(d.clone());
            }
        }

        while let Some(current) = queue.pop_front() {
            for d in self.direct_from(&current) {
                if d != origin && visited.insert(d.clone()) {
                    queue.push_back(d.clone());
                }
            }
        }

        visited
    }

    /// All unique destinations across every edge.
    pub fn all_destinations(&self) -> HashSet<LocationId> {
        self.adj
            .values()
            .flat_map(|dests| dests.iter().cloned())
            .collect()
    }

    /// Destinations ordered by BFS distance from `origin`.
    ///
    /// Returns destinations in topological order: nearest first, farthest last.
    /// This ensures chain transfers work correctly — e.g., `cloud` is processed
    /// before `pod` when the graph is `local→cloud→pod`.
    ///
    /// Does **not** include `origin` itself.
    pub fn destinations_ordered_from(&self, origin: &LocationId) -> Vec<LocationId> {
        let mut result = Vec::new();
        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();

        for d in self.direct_from(origin) {
            if visited.insert(d.clone()) {
                queue.push_back(d.clone());
                result.push(d.clone());
            }
        }

        while let Some(current) = queue.pop_front() {
            for d in self.direct_from(&current) {
                if d != origin && visited.insert(d.clone()) {
                    queue.push_back(d.clone());
                    result.push(d.clone());
                }
            }
        }

        result
    }

    /// Number of edges.
    pub fn edge_count(&self) -> usize {
        self.adj.values().map(|dests| dests.len()).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn loc(s: &str) -> LocationId {
        LocationId::new(s).unwrap()
    }

    #[test]
    fn add_and_has() {
        let mut g = RouteGraph::new();
        g.add(loc("local"), loc("cloud"));
        assert!(g.has(&loc("local"), &loc("cloud")));
        assert!(!g.has(&loc("cloud"), &loc("local")));
    }

    #[test]
    fn self_loop_ignored() {
        let mut g = RouteGraph::new();
        g.add(loc("local"), loc("local"));
        assert_eq!(g.edge_count(), 0);
    }

    #[test]
    fn remove_edge() {
        let mut g = RouteGraph::new();
        g.add(loc("local"), loc("cloud"));
        g.remove(&loc("local"), &loc("cloud"));
        assert!(!g.has(&loc("local"), &loc("cloud")));
        assert_eq!(g.edge_count(), 0);
    }

    #[test]
    fn direct_from() {
        let mut g = RouteGraph::new();
        g.add(loc("local"), loc("cloud"));
        g.add(loc("local"), loc("pod"));
        g.add(loc("cloud"), loc("local"));

        let direct: HashSet<LocationId> = g.direct_from(&loc("local")).cloned().collect();
        assert_eq!(direct.len(), 2);
        assert!(direct.contains(&loc("cloud")));
        assert!(direct.contains(&loc("pod")));
    }

    #[test]
    fn reachable_direct_only() {
        let mut g = RouteGraph::new();
        g.add(loc("local"), loc("cloud"));

        let r = g.reachable_from(&loc("local"));
        assert_eq!(r, HashSet::from([loc("cloud")]));
    }

    #[test]
    fn reachable_multi_hop() {
        let mut g = RouteGraph::new();
        g.add(loc("local"), loc("cloud"));
        g.add(loc("cloud"), loc("pod"));

        let r = g.reachable_from(&loc("local"));
        assert_eq!(r, HashSet::from([loc("cloud"), loc("pod")]));
    }

    #[test]
    fn reachable_excludes_origin() {
        let mut g = RouteGraph::new();
        g.add(loc("local"), loc("cloud"));
        g.add(loc("cloud"), loc("local"));

        let r = g.reachable_from(&loc("local"));
        assert_eq!(r, HashSet::from([loc("cloud")]));
        assert!(!r.contains(&loc("local")));
    }

    #[test]
    fn reachable_isolated_node() {
        let mut g = RouteGraph::new();
        g.add(loc("local"), loc("cloud"));
        // pod has no inbound edges from local or cloud
        g.add(loc("pod"), loc("cloud"));

        let r = g.reachable_from(&loc("local"));
        assert_eq!(r, HashSet::from([loc("cloud")]));
        assert!(!r.contains(&loc("pod")));
    }

    #[test]
    fn reachable_diamond() {
        //   local → cloud → pod
        //   local → nas   → pod
        let mut g = RouteGraph::new();
        g.add(loc("local"), loc("cloud"));
        g.add(loc("local"), loc("nas"));
        g.add(loc("cloud"), loc("pod"));
        g.add(loc("nas"), loc("pod"));

        let r = g.reachable_from(&loc("local"));
        assert_eq!(r, HashSet::from([loc("cloud"), loc("nas"), loc("pod")]));
    }

    #[test]
    fn reachable_empty_graph() {
        let g = RouteGraph::new();
        let r = g.reachable_from(&loc("local"));
        assert!(r.is_empty());
    }

    #[test]
    fn all_destinations() {
        let mut g = RouteGraph::new();
        g.add(loc("local"), loc("cloud"));
        g.add(loc("pod"), loc("cloud"));

        let dests = g.all_destinations();
        assert_eq!(dests, HashSet::from([loc("cloud")]));
    }

    #[test]
    fn destinations_ordered_chain() {
        // local → cloud → pod: cloud must come before pod
        let mut g = RouteGraph::new();
        g.add(loc("local"), loc("cloud"));
        g.add(loc("cloud"), loc("pod"));

        let ordered = g.destinations_ordered_from(&loc("local"));
        assert_eq!(ordered.len(), 2);
        assert_eq!(ordered[0], loc("cloud"));
        assert_eq!(ordered[1], loc("pod"));
    }

    #[test]
    fn destinations_ordered_diamond() {
        // local → cloud, local → nas, cloud → pod, nas → pod
        let mut g = RouteGraph::new();
        g.add(loc("local"), loc("cloud"));
        g.add(loc("local"), loc("nas"));
        g.add(loc("cloud"), loc("pod"));
        g.add(loc("nas"), loc("pod"));

        let ordered = g.destinations_ordered_from(&loc("local"));
        // cloud and nas at depth 1 (order between them is unspecified),
        // pod at depth 2 (must be last)
        assert_eq!(ordered.len(), 3);
        assert_eq!(ordered[2], loc("pod"));
        assert!(ordered[..2].contains(&loc("cloud")));
        assert!(ordered[..2].contains(&loc("nas")));
    }

    #[test]
    fn destinations_ordered_empty_graph() {
        let g = RouteGraph::new();
        let ordered = g.destinations_ordered_from(&loc("local"));
        assert!(ordered.is_empty());
    }
}

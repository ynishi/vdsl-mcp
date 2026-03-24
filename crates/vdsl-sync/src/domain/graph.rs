//! RouteGraph — directed weighted graph of location-to-location transfer topology.
//!
//! Pure domain value object. Knows which locations can reach which others
//! and the estimated cost of each edge.
//!
//! # Optimal Transfer Tree
//!
//! Given an origin and a set of required destinations, [`optimal_tree`]
//! computes the minimum-cost set of edges (approximate Steiner Tree)
//! that delivers a file from origin to all destinations.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};

use super::location::LocationId;

/// Transfer cost for an edge.
///
/// Represents the estimated cost of transferring data along this route.
/// Lower total cost = preferred path.
#[derive(Debug, Clone, Copy)]
pub struct EdgeCost {
    /// Estimated seconds per GB for this route.
    /// Used for transfer time estimation.
    pub time_per_gb: f64,
    /// Static priority (lower = preferred when costs are equal).
    pub priority: u32,
}

impl EdgeCost {
    pub fn new(time_per_gb: f64, priority: u32) -> Result<Self, super::error::DomainError> {
        if !time_per_gb.is_finite() || time_per_gb < 0.0 {
            return Err(super::error::DomainError::Validation {
                field: "time_per_gb".to_string(),
                reason: format!("must be finite and non-negative, got {time_per_gb}"),
            });
        }
        Ok(Self {
            time_per_gb,
            priority,
        })
    }

    /// Scalar cost for path comparison.
    /// Combines time estimate and priority into a single comparable value.
    fn scalar(&self) -> f64 {
        self.time_per_gb + (self.priority as f64) * 0.001
    }
}

impl Default for EdgeCost {
    fn default() -> Self {
        Self {
            time_per_gb: 1.0,
            priority: 100,
        }
    }
}

/// Directed weighted graph of transfer reachability between locations.
///
/// Each edge `(src, dest, cost)` means "a file present at `src` can be
/// transferred to `dest` with estimated cost `cost`".
///
/// # Invariants
///
/// - Self-loops are silently rejected (`add` ignores `src == dest`).
/// - Duplicate edges keep the latest cost.
///
/// # Data structure
///
/// Adjacency list (`HashMap<src, HashMap<dest, EdgeCost>>`) — O(1) lookup.
#[derive(Debug, Clone, Default)]
pub struct RouteGraph {
    adj: HashMap<LocationId, HashMap<LocationId, EdgeCost>>,
}

impl RouteGraph {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a directed edge with default cost. Self-loops are silently ignored.
    pub fn add(&mut self, src: LocationId, dest: LocationId) {
        self.add_with_cost(src, dest, EdgeCost::default());
    }

    /// Add a directed edge with explicit cost. Self-loops are silently ignored.
    pub fn add_with_cost(&mut self, src: LocationId, dest: LocationId, cost: EdgeCost) {
        if src != dest {
            self.adj.entry(src).or_default().insert(dest, cost);
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
        self.adj
            .get(src)
            .is_some_and(|dests| dests.contains_key(dest))
    }

    /// Get the cost of an edge, if it exists.
    pub fn edge_cost(&self, src: &LocationId, dest: &LocationId) -> Option<&EdgeCost> {
        self.adj.get(src).and_then(|dests| dests.get(dest))
    }

    /// Direct neighbors reachable from `origin` (1-hop).
    pub fn direct_from(&self, origin: &LocationId) -> impl Iterator<Item = &LocationId> {
        self.adj
            .get(origin)
            .into_iter()
            .flat_map(|dests| dests.keys())
    }

    /// All locations reachable from `origin` via BFS (multi-hop).
    ///
    /// The result does **not** include `origin` itself, even if there is a
    /// cycle back to it.
    pub fn reachable_from(&self, origin: &LocationId) -> HashSet<LocationId> {
        self.bfs_from(origin).into_iter().collect()
    }

    /// All unique destinations across every edge.
    pub fn all_destinations(&self) -> HashSet<LocationId> {
        self.adj
            .values()
            .flat_map(|dests| dests.keys().cloned())
            .collect()
    }

    /// Destinations ordered by BFS distance from `origin`.
    ///
    /// Returns destinations in topological order: nearest first, farthest last.
    /// Does **not** include `origin` itself.
    pub fn destinations_ordered_from(&self, origin: &LocationId) -> Vec<LocationId> {
        self.bfs_from(origin)
    }

    /// Compute the optimal transfer tree (approximate Steiner Tree).
    ///
    /// Given `origin` and `required_dests`, returns the minimum-cost set
    /// of directed edges that delivers from origin to ALL required destinations.
    ///
    /// Algorithm: Dijkstra from origin, then trace back shortest paths to each
    /// required destination, merging shared edges (counted once).
    ///
    /// Returns edges in dependency order: if edge (A,B) must complete before
    /// (B,C) can start, (A,B) comes first in the result.
    ///
    /// For N <= 10 locations this is optimal. For larger graphs it's a
    /// 2-approximation of the Steiner Tree.
    pub fn optimal_tree(
        &self,
        origin: &LocationId,
        required_dests: &HashSet<LocationId>,
    ) -> Vec<(LocationId, LocationId)> {
        if required_dests.is_empty() {
            return Vec::new();
        }

        // Dijkstra: compute shortest path from origin to all reachable nodes
        let (dist, prev) = self.dijkstra(origin);

        // Collect edges by tracing back from each required dest
        let mut tree_edges: HashSet<(LocationId, LocationId)> = HashSet::new();
        for dest in required_dests {
            let mut current = dest.clone();
            while let Some(predecessor) = prev.get(&current) {
                tree_edges.insert((predecessor.clone(), current.clone()));
                current = predecessor.clone();
            }
            // If current != origin and we didn't reach origin, dest is unreachable
        }

        // Topological sort: BFS from origin through tree_edges only
        let mut result = Vec::with_capacity(tree_edges.len());
        let mut visited = HashSet::new();
        visited.insert(origin.clone());
        let mut queue = VecDeque::new();
        queue.push_back(origin.clone());

        while let Some(node) = queue.pop_front() {
            // Find all tree edges from this node
            let outgoing: Vec<_> = tree_edges
                .iter()
                .filter(|(src, _)| src == &node)
                .cloned()
                .collect();

            // Sort by cost for deterministic output
            let mut outgoing = outgoing;
            outgoing.sort_by(|(_, d1), (_, d2)| {
                let c1 = dist.get(d1).copied().unwrap_or(f64::INFINITY);
                let c2 = dist.get(d2).copied().unwrap_or(f64::INFINITY);
                c1.partial_cmp(&c2).unwrap_or(Ordering::Equal)
            });

            for (src, dest) in outgoing {
                if visited.insert(dest.clone()) {
                    result.push((src, dest.clone()));
                    queue.push_back(dest);
                }
            }
        }

        result
    }

    /// Compute the optimal transfer tree from multiple sources.
    ///
    /// All locations in `sources` already have the data. Finds the minimum-cost
    /// edges to deliver to all `required_dests` (excluding any that are already
    /// in `sources`).
    ///
    /// Uses multi-source Dijkstra: all sources start at distance 0.
    /// This picks the cheapest source→dest path across all sources.
    ///
    /// Example: sources={local, pod}, targets={cloud}
    ///   pod→cloud(2.0) < local→cloud(5.0) → picks pod→cloud.
    pub fn optimal_tree_multi_source(
        &self,
        sources: &HashSet<LocationId>,
        required_dests: &HashSet<LocationId>,
    ) -> Vec<(LocationId, LocationId)> {
        if sources.is_empty() || required_dests.is_empty() {
            return Vec::new();
        }

        // Filter out targets that are already sources (they already have data)
        let actual_dests: HashSet<LocationId> = required_dests
            .iter()
            .filter(|d| !sources.contains(d))
            .cloned()
            .collect();

        if actual_dests.is_empty() {
            return Vec::new();
        }

        // Multi-source Dijkstra
        let (dist, prev) = self.dijkstra_multi_source(sources);

        // Trace back from each required dest
        let mut tree_edges: HashSet<(LocationId, LocationId)> = HashSet::new();
        for dest in &actual_dests {
            let mut current = dest.clone();
            while let Some(predecessor) = prev.get(&current) {
                tree_edges.insert((predecessor.clone(), current.clone()));
                current = predecessor.clone();
            }
        }

        // Topological sort: BFS from all sources through tree_edges only
        let mut result = Vec::with_capacity(tree_edges.len());
        let mut visited: HashSet<LocationId> = sources.clone();
        let mut queue: VecDeque<LocationId> = sources.iter().cloned().collect();

        while let Some(node) = queue.pop_front() {
            let mut outgoing: Vec<_> = tree_edges
                .iter()
                .filter(|(src, _)| src == &node)
                .cloned()
                .collect();

            outgoing.sort_by(|(_, d1), (_, d2)| {
                let c1 = dist.get(d1).copied().unwrap_or(f64::INFINITY);
                let c2 = dist.get(d2).copied().unwrap_or(f64::INFINITY);
                c1.partial_cmp(&c2).unwrap_or(Ordering::Equal)
            });

            for (src, dest) in outgoing {
                if visited.insert(dest.clone()) {
                    result.push((src, dest.clone()));
                    queue.push_back(dest);
                }
            }
        }

        result
    }

    /// Dijkstra's algorithm from a single origin.
    ///
    /// Returns (distances, predecessors).
    fn dijkstra(
        &self,
        origin: &LocationId,
    ) -> (HashMap<LocationId, f64>, HashMap<LocationId, LocationId>) {
        let sources = HashSet::from([origin.clone()]);
        self.dijkstra_multi_source(&sources)
    }

    /// Multi-source Dijkstra's algorithm.
    ///
    /// All sources start at distance 0. Returns (distances, predecessors) where:
    /// - distances: HashMap<LocationId, f64> — shortest distance from any source
    /// - predecessors: HashMap<LocationId, LocationId> — previous node on shortest path
    fn dijkstra_multi_source(
        &self,
        sources: &HashSet<LocationId>,
    ) -> (HashMap<LocationId, f64>, HashMap<LocationId, LocationId>) {
        let mut dist: HashMap<LocationId, f64> = HashMap::new();
        let mut prev: HashMap<LocationId, LocationId> = HashMap::new();
        let mut heap = BinaryHeap::new();

        for source in sources {
            dist.insert(source.clone(), 0.0);
            heap.push(DijkstraEntry {
                cost: 0.0,
                node: source.clone(),
            });
        }

        while let Some(DijkstraEntry { cost, node }) = heap.pop() {
            // Skip if we've already found a better path
            if let Some(&best) = dist.get(&node) {
                if cost > best {
                    continue;
                }
            }

            if let Some(neighbors) = self.adj.get(&node) {
                for (next, edge_cost) in neighbors {
                    let next_cost = cost + edge_cost.scalar();
                    let is_better = dist
                        .get(next)
                        .is_none_or(|&current_best| next_cost < current_best);

                    if is_better {
                        dist.insert(next.clone(), next_cost);
                        prev.insert(next.clone(), node.clone());
                        heap.push(DijkstraEntry {
                            cost: next_cost,
                            node: next.clone(),
                        });
                    }
                }
            }
        }

        (dist, prev)
    }

    /// BFS traversal from `origin`. Returns destinations in visit order.
    ///
    /// Does **not** include `origin` itself, even if there is a cycle back to it.
    fn bfs_from(&self, origin: &LocationId) -> Vec<LocationId> {
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

    /// All edges as `(src, dest)` pairs.
    pub fn all_edges(&self) -> Vec<(LocationId, LocationId)> {
        let mut edges = Vec::new();
        for (src, dests) in &self.adj {
            for dest in dests.keys() {
                edges.push((src.clone(), dest.clone()));
            }
        }
        edges
    }

    /// Number of edges.
    pub fn edge_count(&self) -> usize {
        self.adj.values().map(|dests| dests.len()).sum()
    }
}

impl super::plan::Topology for RouteGraph {
    fn reachable_from(&self, origin: &LocationId) -> HashSet<LocationId> {
        self.reachable_from(origin)
    }

    fn optimal_tree(
        &self,
        origin: &LocationId,
        required_dests: &HashSet<LocationId>,
    ) -> Vec<(LocationId, LocationId)> {
        self.optimal_tree(origin, required_dests)
    }

    fn optimal_tree_multi_source(
        &self,
        sources: &HashSet<LocationId>,
        required_dests: &HashSet<LocationId>,
    ) -> Vec<(LocationId, LocationId)> {
        self.optimal_tree_multi_source(sources, required_dests)
    }
}

/// Entry for Dijkstra's priority queue (min-heap via Reverse ordering).
#[derive(Debug, Clone)]
struct DijkstraEntry {
    cost: f64,
    node: LocationId,
}

impl PartialEq for DijkstraEntry {
    fn eq(&self, other: &Self) -> bool {
        self.cost == other.cost && self.node == other.node
    }
}

impl Eq for DijkstraEntry {}

impl PartialOrd for DijkstraEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DijkstraEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse order for min-heap (BinaryHeap is max-heap)
        other
            .cost
            .partial_cmp(&self.cost)
            .unwrap_or(Ordering::Equal)
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
        g.add(loc("pod"), loc("cloud"));

        let r = g.reachable_from(&loc("local"));
        assert_eq!(r, HashSet::from([loc("cloud")]));
        assert!(!r.contains(&loc("pod")));
    }

    #[test]
    fn reachable_diamond() {
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
        let mut g = RouteGraph::new();
        g.add(loc("local"), loc("cloud"));
        g.add(loc("local"), loc("nas"));
        g.add(loc("cloud"), loc("pod"));
        g.add(loc("nas"), loc("pod"));

        let ordered = g.destinations_ordered_from(&loc("local"));
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

    // =========================================================================
    // optimal_tree tests
    // =========================================================================

    #[test]
    fn optimal_tree_chain_prefers_single_path() {
        // Graph: local→pod (cost 1.0), pod→cloud (cost 2.0), local→cloud (cost 10.0)
        // Required: {pod, cloud}
        // Optimal: local→pod→cloud (cost 3.0) beats local→pod + local→cloud (cost 11.0)
        let mut g = RouteGraph::new();
        g.add_with_cost(loc("local"), loc("pod"), EdgeCost::new(1.0, 10).unwrap());
        g.add_with_cost(loc("pod"), loc("cloud"), EdgeCost::new(2.0, 10).unwrap());
        g.add_with_cost(loc("local"), loc("cloud"), EdgeCost::new(10.0, 10).unwrap());

        let required = HashSet::from([loc("pod"), loc("cloud")]);
        let tree = g.optimal_tree(&loc("local"), &required);

        assert_eq!(tree.len(), 2);
        assert_eq!(tree[0], (loc("local"), loc("pod")));
        assert_eq!(tree[1], (loc("pod"), loc("cloud")));
    }

    #[test]
    fn optimal_tree_direct_cheaper() {
        // Graph: local→pod (cost 10.0), pod→cloud (cost 10.0), local→cloud (cost 1.0)
        // Required: {pod, cloud}
        // Each dest needs its own path: local→pod (10.0), local→cloud (1.0)
        let mut g = RouteGraph::new();
        g.add_with_cost(loc("local"), loc("pod"), EdgeCost::new(10.0, 10).unwrap());
        g.add_with_cost(loc("pod"), loc("cloud"), EdgeCost::new(10.0, 10).unwrap());
        g.add_with_cost(loc("local"), loc("cloud"), EdgeCost::new(1.0, 10).unwrap());

        let required = HashSet::from([loc("pod"), loc("cloud")]);
        let tree = g.optimal_tree(&loc("local"), &required);

        // Both paths needed: local→pod and local→cloud
        assert_eq!(tree.len(), 2);
        let edges: HashSet<_> = tree.into_iter().collect();
        assert!(edges.contains(&(loc("local"), loc("pod"))));
        assert!(edges.contains(&(loc("local"), loc("cloud"))));
    }

    #[test]
    fn optimal_tree_single_dest() {
        let mut g = RouteGraph::new();
        g.add_with_cost(loc("local"), loc("cloud"), EdgeCost::new(5.0, 10).unwrap());

        let required = HashSet::from([loc("cloud")]);
        let tree = g.optimal_tree(&loc("local"), &required);

        assert_eq!(tree.len(), 1);
        assert_eq!(tree[0], (loc("local"), loc("cloud")));
    }

    #[test]
    fn optimal_tree_empty_dests() {
        let mut g = RouteGraph::new();
        g.add(loc("local"), loc("cloud"));

        let tree = g.optimal_tree(&loc("local"), &HashSet::new());
        assert!(tree.is_empty());
    }

    #[test]
    fn optimal_tree_unreachable_dest_skipped() {
        let mut g = RouteGraph::new();
        g.add(loc("local"), loc("cloud"));
        // pod is not reachable from local

        let required = HashSet::from([loc("cloud"), loc("pod")]);
        let tree = g.optimal_tree(&loc("local"), &required);

        // Only cloud is reachable
        assert_eq!(tree.len(), 1);
        assert_eq!(tree[0], (loc("local"), loc("cloud")));
    }

    #[test]
    fn optimal_tree_dependency_order() {
        // local→pod→cloud: pod edge must come before cloud edge
        let mut g = RouteGraph::new();
        g.add_with_cost(loc("local"), loc("pod"), EdgeCost::new(1.0, 10).unwrap());
        g.add_with_cost(loc("pod"), loc("cloud"), EdgeCost::new(1.0, 10).unwrap());

        let required = HashSet::from([loc("pod"), loc("cloud")]);
        let tree = g.optimal_tree(&loc("local"), &required);

        assert_eq!(tree.len(), 2);
        // local→pod MUST come before pod→cloud
        assert_eq!(tree[0], (loc("local"), loc("pod")));
        assert_eq!(tree[1], (loc("pod"), loc("cloud")));
    }

    #[test]
    fn optimal_tree_diamond_deduplicates() {
        // local→cloud, local→nas, cloud→pod, nas→pod
        // Required: {cloud, nas, pod}
        // pod is reachable via cloud or nas — tree should pick one
        let mut g = RouteGraph::new();
        g.add_with_cost(loc("local"), loc("cloud"), EdgeCost::new(1.0, 10).unwrap());
        g.add_with_cost(loc("local"), loc("nas"), EdgeCost::new(1.0, 10).unwrap());
        g.add_with_cost(loc("cloud"), loc("pod"), EdgeCost::new(1.0, 10).unwrap());
        g.add_with_cost(loc("nas"), loc("pod"), EdgeCost::new(5.0, 10).unwrap());

        let required = HashSet::from([loc("cloud"), loc("nas"), loc("pod")]);
        let tree = g.optimal_tree(&loc("local"), &required);

        // cloud and nas both needed (required). pod via cloud (cheaper).
        // Edges: local→cloud, local→nas, cloud→pod
        assert_eq!(tree.len(), 3);
        let edges: HashSet<_> = tree.into_iter().collect();
        assert!(edges.contains(&(loc("local"), loc("cloud"))));
        assert!(edges.contains(&(loc("local"), loc("nas"))));
        assert!(edges.contains(&(loc("cloud"), loc("pod"))));
        // nas→pod should NOT be included (duplicate, more expensive)
        assert!(!edges.contains(&(loc("nas"), loc("pod"))));
    }

    // =========================================================================
    // optimal_tree_multi_source tests
    // =========================================================================

    #[test]
    fn multi_source_picks_cheaper_relay() {
        // Real scenario: local and pod both have data, need to reach cloud.
        // Routes: local→pod(1.0), pod→cloud(2.0), local→cloud(5.0), cloud→local(5.0), cloud→pod(2.0)
        // Sources: {local, pod} (both already have the file)
        // Targets: {cloud}
        //
        // Single-source from local: local→cloud = 5.0
        // Multi-source: pod→cloud = 2.0 (cheaper!)
        let mut g = RouteGraph::new();
        g.add_with_cost(loc("local"), loc("pod"), EdgeCost::new(1.0, 10).unwrap());
        g.add_with_cost(loc("pod"), loc("cloud"), EdgeCost::new(2.0, 10).unwrap());
        g.add_with_cost(loc("local"), loc("cloud"), EdgeCost::new(5.0, 10).unwrap());
        g.add_with_cost(loc("cloud"), loc("local"), EdgeCost::new(5.0, 10).unwrap());
        g.add_with_cost(loc("cloud"), loc("pod"), EdgeCost::new(2.0, 10).unwrap());

        let sources = HashSet::from([loc("local"), loc("pod")]);
        let targets = HashSet::from([loc("cloud")]);
        let tree = g.optimal_tree_multi_source(&sources, &targets);

        assert_eq!(tree.len(), 1);
        assert_eq!(tree[0], (loc("pod"), loc("cloud")));
    }

    #[test]
    fn multi_source_single_source_fallback() {
        // Single source behaves like original optimal_tree.
        let mut g = RouteGraph::new();
        g.add_with_cost(loc("local"), loc("pod"), EdgeCost::new(1.0, 10).unwrap());
        g.add_with_cost(loc("pod"), loc("cloud"), EdgeCost::new(2.0, 10).unwrap());

        let sources = HashSet::from([loc("local")]);
        let targets = HashSet::from([loc("pod"), loc("cloud")]);
        let tree = g.optimal_tree_multi_source(&sources, &targets);

        assert_eq!(tree.len(), 2);
        assert_eq!(tree[0], (loc("local"), loc("pod")));
        assert_eq!(tree[1], (loc("pod"), loc("cloud")));
    }

    #[test]
    fn multi_source_empty_sources() {
        let mut g = RouteGraph::new();
        g.add(loc("local"), loc("cloud"));

        let tree = g.optimal_tree_multi_source(&HashSet::new(), &HashSet::from([loc("cloud")]));
        assert!(tree.is_empty());
    }

    #[test]
    fn multi_source_empty_targets() {
        let mut g = RouteGraph::new();
        g.add(loc("local"), loc("cloud"));

        let tree = g.optimal_tree_multi_source(&HashSet::from([loc("local")]), &HashSet::new());
        assert!(tree.is_empty());
    }

    #[test]
    fn multi_source_target_already_in_sources() {
        // If cloud is both a source and a target, no transfer needed.
        let mut g = RouteGraph::new();
        g.add_with_cost(loc("local"), loc("cloud"), EdgeCost::new(5.0, 10).unwrap());

        let sources = HashSet::from([loc("local"), loc("cloud")]);
        let targets = HashSet::from([loc("cloud")]);
        let tree = g.optimal_tree_multi_source(&sources, &targets);

        assert!(
            tree.is_empty(),
            "target already has data, no transfer needed"
        );
    }

    #[test]
    fn multi_source_multiple_targets_different_best_sources() {
        // local has data, pod has data.
        // Routes: local→nas(1.0), pod→cloud(2.0), local→cloud(10.0), pod→nas(10.0)
        // sources={local, pod}, targets={nas, cloud}
        //
        // Best: local→nas(1.0), pod→cloud(2.0) — each target from different source
        let mut g = RouteGraph::new();
        g.add_with_cost(loc("local"), loc("nas"), EdgeCost::new(1.0, 10).unwrap());
        g.add_with_cost(loc("pod"), loc("cloud"), EdgeCost::new(2.0, 10).unwrap());
        g.add_with_cost(loc("local"), loc("cloud"), EdgeCost::new(10.0, 10).unwrap());
        g.add_with_cost(loc("pod"), loc("nas"), EdgeCost::new(10.0, 10).unwrap());

        let sources = HashSet::from([loc("local"), loc("pod")]);
        let targets = HashSet::from([loc("nas"), loc("cloud")]);
        let tree = g.optimal_tree_multi_source(&sources, &targets);

        assert_eq!(tree.len(), 2);
        let edges: HashSet<_> = tree.into_iter().collect();
        assert!(edges.contains(&(loc("local"), loc("nas"))));
        assert!(edges.contains(&(loc("pod"), loc("cloud"))));
    }
}

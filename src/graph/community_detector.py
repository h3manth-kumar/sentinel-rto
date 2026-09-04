"""Louvain community detection for syndicate ring identification.

Detects fraud rings by clustering the entity graph into communities
and computing aggregate risk metrics per cluster.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import networkx as nx
from networkx.algorithms.community import louvain_communities

logger = logging.getLogger(__name__)


@dataclass
class SyndicateClusterResult:
    """Result of community detection for a single cluster."""
    cluster_id: str
    members: set[str]
    cluster_size: int
    root_entity_type: str
    composite_rto_rate: float
    is_suspicious: bool
    metrics: dict[str, Any]


class CommunityDetector:
    """Detects syndicate fraud rings using Louvain community clustering.

    Operates on the entity graph built by EntityResolver to identify
    tightly-connected clusters of accounts, devices, and addresses
    that exhibit coordinated fraud patterns.
    """

    # Thresholds for suspicious cluster identification
    MIN_CLUSTER_SIZE = 3
    SUSPICIOUS_RTO_RATE = 0.4
    HIGH_RISK_RTO_RATE = 0.7

    def __init__(self, graph: nx.Graph) -> None:
        self.graph = graph
        self._communities: list[set[str]] = []
        self._cluster_results: list[SyndicateClusterResult] = []

    def detect_communities(
        self,
        resolution: float = 1.0,
        seed: int = 42,
    ) -> list[SyndicateClusterResult]:
        """Run Louvain community detection on the entity graph.

        Args:
            resolution: Louvain resolution parameter. Higher values
                produce more, smaller communities.
            seed: Random seed for reproducibility.

        Returns:
            List of SyndicateClusterResult for each detected community.
        """
        if self.graph.number_of_nodes() == 0:
            logger.warning("Empty graph, no communities to detect.")
            return []

        self._communities = louvain_communities(
            self.graph,
            resolution=resolution,
            seed=seed,
        )

        self._cluster_results = []
        for idx, community in enumerate(self._communities):
            cluster = self._analyze_cluster(f"clust_{idx:04d}", community)
            self._cluster_results.append(cluster)

        suspicious_count = sum(1 for c in self._cluster_results if c.is_suspicious)
        logger.info(
            "Detected %d communities, %d suspicious (size >= %d, RTO rate >= %.1f%%)",
            len(self._cluster_results),
            suspicious_count,
            self.MIN_CLUSTER_SIZE,
            self.SUSPICIOUS_RTO_RATE * 100,
        )

        return self._cluster_results

    def _analyze_cluster(
        self, cluster_id: str, members: set[str]
    ) -> SyndicateClusterResult:
        """Analyze a single community cluster for risk metrics."""
        subgraph = self.graph.subgraph(members)

        # Count entity types
        type_counts: dict[str, int] = {}
        total_orders = 0
        total_rto = 0

        for node in members:
            node_data = self.graph.nodes[node]
            entity_type = node_data.get("entity_type", "UNKNOWN")
            type_counts[entity_type] = type_counts.get(entity_type, 0) + 1

            # Aggregate order statistics from address nodes
            if entity_type == "ADDRESS":
                total_orders += node_data.get("total_orders", 0)
                total_rto += node_data.get("rto_deliveries", 0)

        # Determine root entity type (most prevalent non-ACCOUNT type)
        non_account_types = {
            k: v for k, v in type_counts.items() if k != "ACCOUNT"
        }
        root_entity_type = (
            max(non_account_types, key=non_account_types.get)
            if non_account_types
            else "DEVICE"
        )

        # Compute composite RTO rate
        composite_rto_rate = (
            total_rto / total_orders if total_orders > 0 else 0.0
        )

        # Determine if cluster is suspicious
        is_suspicious = (
            len(members) >= self.MIN_CLUSTER_SIZE
            and composite_rto_rate >= self.SUSPICIOUS_RTO_RATE
        )

        return SyndicateClusterResult(
            cluster_id=cluster_id,
            members=members,
            cluster_size=len(members),
            root_entity_type=root_entity_type,
            composite_rto_rate=round(composite_rto_rate, 4),
            is_suspicious=is_suspicious,
            metrics={
                "entity_type_distribution": type_counts,
                "internal_edges": subgraph.number_of_edges(),
                "density": nx.density(subgraph) if len(members) > 1 else 0.0,
                "total_orders": total_orders,
                "total_rto": total_rto,
                "account_count": type_counts.get("ACCOUNT", 0),
            },
        )

    def get_suspicious_clusters(self) -> list[SyndicateClusterResult]:
        """Return only clusters flagged as suspicious."""
        return [c for c in self._cluster_results if c.is_suspicious]

    def get_high_risk_clusters(self) -> list[SyndicateClusterResult]:
        """Return clusters with RTO rate above the high-risk threshold."""
        return [
            c for c in self._cluster_results
            if c.composite_rto_rate >= self.HIGH_RISK_RTO_RATE
        ]

    def get_cluster_for_entity(self, entity_id: str) -> SyndicateClusterResult | None:
        """Find which cluster an entity belongs to."""
        for cluster in self._cluster_results:
            if entity_id in cluster.members:
                return cluster
        return None

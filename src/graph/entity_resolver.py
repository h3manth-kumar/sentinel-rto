"""Entity resolution and linkage engine for fraud ring detection.

Links accounts sharing fuzzy addresses, device fingerprints, payment VPAs,
or IP subnets to construct the fraud entity graph.
"""
from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional

import networkx as nx

logger = logging.getLogger(__name__)


class EntityType(str, Enum):
    """Types of entities in the fraud graph."""
    ACCOUNT = "ACCOUNT"
    DEVICE = "DEVICE"
    ADDRESS = "ADDRESS"
    VPA = "VPA"
    IP_SUBNET = "IP_SUBNET"
    PHONE = "PHONE"


class EdgeType(str, Enum):
    """Types of relationships between entities."""
    SAME_DEVICE = "SAME_DEVICE"
    SAME_ADDRESS = "SAME_ADDRESS"
    SAME_VPA = "SAME_VPA"
    SAME_IP_SUBNET = "SAME_IP_SUBNET"
    SAME_PHONE = "SAME_PHONE"
    FUZZY_ADDRESS = "FUZZY_ADDRESS"
    SPATIAL_PROXIMITY = "SPATIAL_PROXIMITY"


@dataclass
class EntityNode:
    """Represents a node in the fraud entity graph."""
    entity_id: str
    entity_type: EntityType
    attributes: dict[str, Any] = field(default_factory=dict)


@dataclass
class EntityEdge:
    """Represents an edge (relationship) between two entity nodes."""
    source_id: str
    target_id: str
    edge_type: EdgeType
    weight: float = 1.0
    metadata: dict[str, Any] = field(default_factory=dict)


class EntityResolver:
    """Builds and manages the fraud entity graph.

    Ingests order, cancellation, and RTO events to create entity nodes
    and link them based on shared attributes (device hash, address H3,
    phone hash, VPA, IP subnet).
    """

    def __init__(self) -> None:
        self.graph: nx.Graph = nx.Graph()
        self._entity_index: dict[str, EntityNode] = {}

    @property
    def node_count(self) -> int:
        return self.graph.number_of_nodes()

    @property
    def edge_count(self) -> int:
        return self.graph.number_of_edges()

    def add_entity(self, node: EntityNode) -> None:
        """Add an entity node to the graph."""
        if node.entity_id not in self._entity_index:
            self.graph.add_node(
                node.entity_id,
                entity_type=node.entity_type.value,
                **node.attributes,
            )
            self._entity_index[node.entity_id] = node

    def add_edge(self, edge: EntityEdge) -> None:
        """Add a relationship edge between two entities."""
        if edge.source_id not in self._entity_index:
            logger.warning("Source entity %s not found, skipping edge.", edge.source_id)
            return
        if edge.target_id not in self._entity_index:
            logger.warning("Target entity %s not found, skipping edge.", edge.target_id)
            return

        self.graph.add_edge(
            edge.source_id,
            edge.target_id,
            edge_type=edge.edge_type.value,
            weight=edge.weight,
            **edge.metadata,
        )

    def ingest_order_event(
        self,
        order_id: str,
        customer_phone_hash: str,
        device_hash: str,
        h3_index_res9: str,
        ip_address: Optional[str] = None,
        vpa: Optional[str] = None,
    ) -> None:
        """Ingest an order event and resolve entity linkages.

        Creates nodes for account (phone), device, and address entities,
        then links them with appropriate edge types.
        """
        # Create entity nodes
        account_node = EntityNode(
            entity_id=f"account:{customer_phone_hash}",
            entity_type=EntityType.ACCOUNT,
            attributes={"phone_hash": customer_phone_hash},
        )
        device_node = EntityNode(
            entity_id=f"device:{device_hash}",
            entity_type=EntityType.DEVICE,
            attributes={"device_hash": device_hash},
        )
        address_node = EntityNode(
            entity_id=f"address:{h3_index_res9}",
            entity_type=EntityType.ADDRESS,
            attributes={"h3_index": h3_index_res9},
        )

        self.add_entity(account_node)
        self.add_entity(device_node)
        self.add_entity(address_node)

        # Create linkage edges
        self.add_edge(EntityEdge(
            source_id=account_node.entity_id,
            target_id=device_node.entity_id,
            edge_type=EdgeType.SAME_DEVICE,
            metadata={"order_id": order_id},
        ))
        self.add_edge(EntityEdge(
            source_id=account_node.entity_id,
            target_id=address_node.entity_id,
            edge_type=EdgeType.SAME_ADDRESS,
            metadata={"order_id": order_id},
        ))

        # Optional: IP subnet linkage
        if ip_address:
            subnet = self._extract_subnet(ip_address)
            subnet_node = EntityNode(
                entity_id=f"ip_subnet:{subnet}",
                entity_type=EntityType.IP_SUBNET,
                attributes={"subnet": subnet},
            )
            self.add_entity(subnet_node)
            self.add_edge(EntityEdge(
                source_id=account_node.entity_id,
                target_id=subnet_node.entity_id,
                edge_type=EdgeType.SAME_IP_SUBNET,
            ))

        # Optional: VPA linkage
        if vpa:
            vpa_hash = hashlib.sha256(vpa.encode()).hexdigest()[:16]
            vpa_node = EntityNode(
                entity_id=f"vpa:{vpa_hash}",
                entity_type=EntityType.VPA,
                attributes={"vpa_hash": vpa_hash},
            )
            self.add_entity(vpa_node)
            self.add_edge(EntityEdge(
                source_id=account_node.entity_id,
                target_id=vpa_node.entity_id,
                edge_type=EdgeType.SAME_VPA,
            ))

        logger.debug(
            "Ingested order %s: nodes=%d, edges=%d",
            order_id, self.node_count, self.edge_count,
        )

    def get_entity_neighbors(self, entity_id: str) -> list[dict[str, Any]]:
        """Get all neighbors of an entity with edge metadata."""
        if entity_id not in self.graph:
            return []
        neighbors = []
        for neighbor_id in self.graph.neighbors(entity_id):
            edge_data = self.graph.edges[entity_id, neighbor_id]
            node_data = self.graph.nodes[neighbor_id]
            neighbors.append({
                "entity_id": neighbor_id,
                "entity_type": node_data.get("entity_type"),
                "edge_type": edge_data.get("edge_type"),
                "weight": edge_data.get("weight", 1.0),
            })
        return neighbors

    def get_connected_accounts(self, entity_id: str) -> set[str]:
        """Find all account nodes connected to a given entity (BFS)."""
        if entity_id not in self.graph:
            return set()
        connected = set()
        for node in nx.node_connected_component(self.graph, entity_id):
            if self.graph.nodes[node].get("entity_type") == EntityType.ACCOUNT.value:
                connected.add(node)
        return connected

    @staticmethod
    def _extract_subnet(ip_address: str) -> str:
        """Extract /24 subnet from an IP address."""
        parts = ip_address.split(".")
        if len(parts) == 4:
            return f"{parts[0]}.{parts[1]}.{parts[2]}.0/24"
        return ip_address

    def export_graph_data(self) -> dict[str, Any]:
        """Export graph for serialization or visualization."""
        return {
            "nodes": [
                {"id": n, **self.graph.nodes[n]} for n in self.graph.nodes
            ],
            "edges": [
                {"source": u, "target": v, **d}
                for u, v, d in self.graph.edges(data=True)
            ],
            "stats": {
                "node_count": self.node_count,
                "edge_count": self.edge_count,
                "connected_components": nx.number_connected_components(self.graph),
            },
        }

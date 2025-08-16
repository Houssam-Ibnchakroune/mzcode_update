from typing import Dict, Any, Optional
from pydantic import BaseModel, Field


class Node(BaseModel):
    """Represents a node in the knowledge graph with canonical properties"""

    node_id: str
    node_type: str
    name: str
    properties: Dict[str, Any] = Field(default_factory=dict)
    context: Dict[str, Any] = Field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Returns a dictionary representation of the node."""
        d = self.model_dump()
        d["id"] = d.pop("node_id")
        return d

    def __repr__(self) -> str:
        return f"Node(id={self.node_id}, type={self.node_type}, name={self.name})"


class Edge(BaseModel):
    """Represents a relationship between nodes with canonical type"""

    source_id: str
    target_id: str
    relation: str
    properties: Dict[str, Any] = Field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Returns a dictionary representation of the edge."""
        return self.model_dump()

    def __repr__(self) -> str:
        return f"Edge({self.source_id} -> {self.target_id} [{self.relation}])"

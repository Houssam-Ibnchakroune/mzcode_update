"""
Sparse Hierarchical Entity Indexing System

This module implements a 4-level indexing system based on the LocAgent paper
for efficient search across canonical graph entities. The system is completely
language-agnostic and works with any canonical graph structure.

Level 1: Entity ID Index (exact match on IDs)
Level 2: Entity Name Index (exact match on names)
Level 3: BM25 Fuzzy Index on Key Metadata (fuzzy match on important properties)
Level 4: BM25 Deep Content Index (full-text search on all properties)
"""

from typing import Dict, List, Any, Optional, Set, Tuple
import re
from collections import defaultdict

try:
    from rank_bm25 import BM25Okapi
except ImportError:
    BM25Okapi = None

from ..graph.graph_client_interface import GraphClientInterface
from ..models.graph import Node


class HierarchicalEntityIndex:
    """
    Sparse Hierarchical Entity Indexing System based on the LocAgent paper.
    Provides 4 levels of indexing for efficient entity search across any canonical graph.

    This implementation is completely language-agnostic and depends only on the
    universal canonical graph structure, not on the source programming language.
    """

    def __init__(self, graph_client: GraphClientInterface):
        """
        Initialize the hierarchical index from a graph client.

        Args:
            graph_client: Any graph client implementing the GraphClientInterface
        """
        if BM25Okapi is None:
            raise ImportError(
                "BM25 functionality requires 'rank_bm25' package. Install with: pip install rank_bm25"
            )

        self.graph_client = graph_client

        # Level 1: Entity ID Index (hash map for O(1) lookup)
        self.id_index: Dict[str, Node] = {}

        # Level 2: Entity Name Index (hash map for O(1) lookup by name)
        self.name_index: Dict[str, List[Node]] = defaultdict(list)

        # Level 3: BM25 Fuzzy Index on Key Metadata
        self.metadata_documents: List[List[str]] = []
        self.metadata_node_map: List[Node] = []
        self.metadata_bm25: Optional[BM25Okapi] = None

        # Level 4: BM25 Deep Content Index (will be implemented in next milestone)
        self.content_documents: List[List[str]] = []
        self.content_node_map: List[Node] = []
        self.content_bm25: Optional[Any] = None

        # Project identifier (for batch processing)
        self.project_id: Optional[str] = None

        # Build the index
        self._build_index()

    def _get_key_properties_for_node_type(self, node_type: str) -> List[str]:
        """
        Get the key properties to index for each node type.

        Args:
            node_type: The type of node (e.g., 'pipeline', 'operation', 'table')

        Returns:
            List of property names to extract for metadata indexing
        """
        # Define key properties for each node type based on canonical model
        key_properties_map = {
            "pipeline": ["technology", "file_path", "execution_context"],
            "operation": ["native_type", "operation_subtype", "technology"],
            "table": ["schema", "table_type", "technology", "columns"],
            "connection": ["connection_type", "technology", "server", "database"],
            "parameter": ["data_type", "scope", "description", "value"],
            "variable": ["data_type", "scope", "namespace", "expression"],
            "directory": ["path", "technology"],
            "file": ["file_type", "technology", "path"],
            "data_asset": ["asset_type", "technology", "format"],
            "schema": ["database", "technology"],
            "column": ["data_type", "table", "nullable"],
            "entity": ["entity_type", "technology"],
            "transformation": ["transformation_type", "technology", "logic"],
            # Phase 3: Summary node types for enhanced search
            "operation_summary": ["summary_text", "original_node_type", "confidence"],
            "pipeline_summary": ["summary_text", "original_node_type", "confidence"],
        }

        # Return specific properties for the node type, or default set for unknown types
        return key_properties_map.get(node_type.lower(), ["technology", "type"])

    def _extract_metadata_tokens(self, node: Node) -> List[str]:
        """
        Extract and tokenize metadata from a node for Level 3 indexing.

        Args:
            node: The node to extract metadata from

        Returns:
            List of tokens for BM25 indexing
        """
        tokens = []

        # Always include node name and type
        if hasattr(node, "name") and node.name:
            tokens.extend(self._tokenize_text(str(node.name)))

        tokens.extend(self._tokenize_text(str(node.node_type)))

        # Get key properties for this node type
        key_properties = self._get_key_properties_for_node_type(node.node_type)

        # Extract tokens from key properties
        for prop_name in key_properties:
            if hasattr(node, "properties") and prop_name in node.properties:
                prop_value = node.properties[prop_name]
                if prop_value:
                    tokens.extend(self._tokenize_text(str(prop_value)))

            # Also check context dict
            if hasattr(node, "context") and prop_name in node.context:
                context_value = node.context[prop_name]
                if context_value:
                    tokens.extend(self._tokenize_text(str(context_value)))

        # For operations, also include special business logic properties
        if node.node_type.lower() == "operation" and hasattr(node, "properties"):
            business_logic_props = [
                "transformations",
                "conditions",
                "lookups",
                "embedded_scripts",
            ]
            for prop_name in business_logic_props:
                if prop_name in node.properties:
                    prop_value = node.properties[prop_name]
                    if isinstance(prop_value, list):
                        for item in prop_value:
                            tokens.extend(self._tokenize_text(str(item)))
                    elif prop_value:
                        tokens.extend(self._tokenize_text(str(prop_value)))

        # Phase 3: Enhanced handling for summary nodes
        if node.node_type.lower().endswith("_summary") and hasattr(node, "properties"):
            # For summary nodes, give extra weight to business-oriented content
            business_content_props = [
                "summary_text",
                "business_purpose",
                "technical_summary",
                "data_flow_description",
                "impact_analysis",
                "business_context",
            ]
            for prop_name in business_content_props:
                if prop_name in node.properties:
                    prop_value = node.properties[prop_name]
                    if prop_value:
                        # Add summary content tokens multiple times for higher weight
                        summary_tokens = self._tokenize_text(str(prop_value))
                        tokens.extend(summary_tokens)
                        tokens.extend(
                            summary_tokens
                        )  # Double weight for business content

        # Remove empty tokens but preserve duplicates for proper BM25 term frequency
        # Phase 3: Keep duplicates to maintain summary content weighting
        return [token for token in tokens if token.strip()]

    def _tokenize_text(self, text: str) -> List[str]:
        """
        Tokenize text by splitting on various delimiters and handling camelCase/snake_case.

        Args:
            text: Text to tokenize

        Returns:
            List of tokens
        """
        if not text:
            return []

        # Split camelCase and PascalCase BEFORE converting to lowercase
        # This handles cases like "dataFlowTask" -> "data Flow Task"
        text = re.sub(r"([a-z])([A-Z])", r"\1 \2", text)

        # Convert to lowercase
        text = text.lower()

        # Split on common delimiters: space, underscore, hyphen, dot, slash, etc.
        tokens = re.split(r"[\s_\-\.\/\\:;,\(\)\[\]{}]+", text)

        # Filter out empty tokens and very short tokens (< 2 chars)
        tokens = [token.strip() for token in tokens if len(token.strip()) >= 2]

        return tokens

    def _build_index(self) -> None:
        """Build all levels of the index from the graph."""
        nodes = self.graph_client.get_all_nodes()

        # Reset document lists
        self.metadata_documents = []
        self.metadata_node_map = []
        self.content_documents = []
        self.content_node_map = []

        for node in nodes:
            # Level 1: ID Index - Direct hash map lookup
            self.id_index[node.node_id] = node

            # Level 2: Name Index - Case-insensitive name lookup
            if hasattr(node, "name") and node.name:
                name = str(node.name).lower().strip()
                if name:  # Only index non-empty names
                    self.name_index[name].append(node)

            # Level 3: Metadata Index - BM25 on key properties
            metadata_tokens = self._extract_metadata_tokens(node)
            self.metadata_documents.append(metadata_tokens)
            self.metadata_node_map.append(node)

            # Level 4: Content Index - BM25 on all properties
            content_tokens = self._extract_content_tokens(node)
            self.content_documents.append(content_tokens)
            self.content_node_map.append(node)

        # Build BM25 index for metadata (Level 3)
        if self.metadata_documents and BM25Okapi:
            try:
                self.metadata_bm25 = BM25Okapi(self.metadata_documents)
            except Exception as e:
                # If BM25 fails, log warning but continue
                print(f"Warning: Failed to build BM25 metadata index: {e}")
                self.metadata_bm25 = None

        # Build BM25 index for content (Level 4)
        if self.content_documents and BM25Okapi:
            try:
                self.content_bm25 = BM25Okapi(self.content_documents)
            except Exception as e:
                # If BM25 fails, log warning but continue
                print(f"Warning: Failed to build BM25 content index: {e}")
                self.content_bm25 = None

    def search_by_id(self, entity_id: str) -> Optional[Node]:
        """
        Level 1: Search for an entity by exact ID match.

        Args:
            entity_id: The exact node ID to search for

        Returns:
            The node with matching ID, or None if not found
        """
        return self.id_index.get(entity_id)

    def search_by_name(self, name: str) -> List[Node]:
        """
        Level 2: Search for entities by exact name match (case-insensitive).

        Args:
            name: The name to search for

        Returns:
            List of nodes with matching names
        """
        normalized_name = name.lower().strip()
        return self.name_index.get(normalized_name, [])

    def search_by_metadata(
        self, query: str, top_k: int = 10
    ) -> List[Tuple[Node, float]]:
        """
        Level 3: Search for entities by fuzzy matching on key metadata.

        Args:
            query: The search query
            top_k: Maximum number of results to return

        Returns:
            List of (node, relevance_score) tuples sorted by relevance
        """
        if not self.metadata_bm25 or not self.metadata_documents:
            return []

        # Tokenize the query using the same tokenization as documents
        query_tokens = self._tokenize_text(query)
        if not query_tokens:
            return []

        try:
            # Get BM25 scores for all documents
            scores = self.metadata_bm25.get_scores(query_tokens)

            # Calculate a reasonable threshold based on the distribution of scores
            # For small corpora, BM25 often returns negative scores for irrelevant docs
            max_score = float(max(scores)) if len(scores) > 0 else 0
            min_score = float(min(scores)) if len(scores) > 0 else 0

            # If all scores are zero, check if any documents actually contain the query terms
            # This can happen when terms are very common (appear in many documents)
            if max_score == 0:
                # Check if any documents contain the query terms
                query_terms_set = set(query_tokens)
                matching_docs = []
                for i, doc_tokens in enumerate(self.metadata_documents):
                    if query_terms_set.intersection(set(doc_tokens)):
                        # Found documents that contain the query terms, give them a small positive score
                        node = self.metadata_node_map[i]
                        matching_docs.append((node, 0.001))

                if matching_docs:
                    # Sort by node_id for consistent ordering when scores are equal
                    matching_docs.sort(key=lambda x: x[0].node_id)
                    return matching_docs[:top_k]
                else:
                    return []

            # Calculate threshold based on score distribution
            if max_score > 0:
                # For positive scores, require at least 10% of the max score
                threshold = max(0.001, max_score * 0.1)
            else:
                # For negative scores (common in small corpora), use a more lenient approach
                # Take scores that are in the top 70% of the range
                score_range = max_score - min_score
                if score_range > 0:
                    threshold = min_score + (score_range * 0.3)  # Bottom 30% threshold
                else:
                    # All scores are the same, just use the max score
                    threshold = max_score

            # Create (node, score) pairs and sort by score
            results = []
            for i, score in enumerate(scores):
                score_val = float(score)
                if score_val >= threshold:
                    node = self.metadata_node_map[i]
                    # Normalize scores to be positive for consistency
                    # For negative scores, shift them to positive range
                    if max_score <= 0:
                        normalized_score = score_val - min_score + 0.001
                    else:
                        normalized_score = max(score_val, 0.001)
                    results.append((node, normalized_score))

            # Sort by score (descending) and return top_k
            results.sort(key=lambda x: x[1], reverse=True)
            return results[:top_k]

        except Exception as e:
            print(f"Warning: BM25 search failed: {e}")
            return []

    def _extract_content_tokens(self, node: Node) -> List[str]:
        """
        Extract all content tokens from a node for Level 4 indexing.
        This includes all properties and nested content for comprehensive search.

        Phase 3: Enhanced for summary nodes with higher weight for summary content.

        Args:
            node: The node to extract content tokens from

        Returns:
            List of tokens representing all content in the node
        """
        tokens = []

        # Add node type and ID
        tokens.extend(self._tokenize_text(node.node_type))
        tokens.extend(self._tokenize_text(node.node_id))

        # Add node name if available
        if hasattr(node, "name") and node.name:
            tokens.extend(self._tokenize_text(str(node.name)))

        # Phase 3: Special handling for summary nodes - prioritize summary content
        if node.node_type.lower().endswith("_summary") and hasattr(node, "properties"):
            # High-priority summary content (3x weight)
            high_priority_props = [
                "summary_text",
                "business_purpose",
                "technical_summary",
            ]
            for prop_name in high_priority_props:
                if prop_name in node.properties:
                    prop_value = node.properties[prop_name]
                    if prop_value:
                        summary_tokens = self._tokenize_text(str(prop_value))
                        # Triple weight for core summary content
                        tokens.extend(summary_tokens)
                        tokens.extend(summary_tokens)
                        tokens.extend(summary_tokens)

            # Medium-priority business content (2x weight)
            medium_priority_props = [
                "data_flow_description",
                "impact_analysis",
                "business_context",
                "key_transformations",
            ]
            for prop_name in medium_priority_props:
                if prop_name in node.properties:
                    prop_value = node.properties[prop_name]
                    if prop_value:
                        content_tokens = self._tokenize_text(str(prop_value))
                        # Double weight for business content
                        tokens.extend(content_tokens)
                        tokens.extend(content_tokens)

            # Extract remaining properties with normal weight
            summary_priority_props = set(high_priority_props + medium_priority_props)
            remaining_props = {
                k: v
                for k, v in node.properties.items()
                if k not in summary_priority_props
            }
            if remaining_props:
                tokens.extend(self._extract_tokens_from_dict(remaining_props))
        else:
            # Standard extraction for non-summary nodes
            if hasattr(node, "properties") and node.properties:
                tokens.extend(self._extract_tokens_from_dict(node.properties))

        # Extract context if available
        if hasattr(node, "context") and node.context:
            tokens.extend(self._extract_tokens_from_dict(node.context))

        # Remove empty tokens but preserve duplicates for proper BM25 term frequency
        # Phase 3: Keep duplicates to maintain summary content weighting
        return [token for token in tokens if token.strip()]

    def _extract_tokens_from_dict(self, data: dict) -> List[str]:
        """
        Recursively extract tokens from a dictionary structure.

        Args:
            data: Dictionary to extract tokens from

        Returns:
            List of tokens from all values in the dictionary
        """
        tokens = []

        for key, value in data.items():
            # Add the key itself as a token
            tokens.extend(self._tokenize_text(str(key)))

            # Process the value based on its type
            if isinstance(value, str):
                tokens.extend(self._tokenize_text(value))
            elif isinstance(value, (int, float, bool)):
                tokens.extend(self._tokenize_text(str(value)))
            elif isinstance(value, list):
                tokens.extend(self._extract_tokens_from_list(value))
            elif isinstance(value, dict):
                # Recursively process nested dictionaries
                tokens.extend(self._extract_tokens_from_dict(value))
            elif value is not None:
                # Handle other types by converting to string
                tokens.extend(self._tokenize_text(str(value)))

        return tokens

    def _extract_tokens_from_list(self, data: list) -> List[str]:
        """
        Extract tokens from a list structure.

        Args:
            data: List to extract tokens from

        Returns:
            List of tokens from all items in the list
        """
        tokens = []

        for item in data:
            if isinstance(item, str):
                tokens.extend(self._tokenize_text(item))
            elif isinstance(item, (int, float, bool)):
                tokens.extend(self._tokenize_text(str(item)))
            elif isinstance(item, dict):
                tokens.extend(self._extract_tokens_from_dict(item))
            elif isinstance(item, list):
                # Recursively process nested lists
                tokens.extend(self._extract_tokens_from_list(item))
            elif item is not None:
                # Handle other types by converting to string
                tokens.extend(self._tokenize_text(str(item)))

        return tokens

    def search_by_content(
        self, query: str, top_k: int = 10
    ) -> List[Tuple[Node, float]]:
        """
        Level 4: Search for entities by full-text search on all properties.

        Args:
            query: The search query
            top_k: Maximum number of results to return

        Returns:
            List of (node, relevance_score) tuples sorted by relevance
        """
        if not self.content_bm25 or not self.content_documents:
            return []

        # Tokenize the query using the same tokenization as documents
        query_tokens = self._tokenize_text(query)
        if not query_tokens:
            return []

        try:
            # Get BM25 scores for all documents
            scores = self.content_bm25.get_scores(query_tokens)

            # Calculate a reasonable threshold based on the distribution of scores
            max_score = float(max(scores)) if len(scores) > 0 else 0
            min_score = float(min(scores)) if len(scores) > 0 else 0

            # If all scores are zero, check if any documents actually contain the query terms
            if max_score == 0:
                # Check if any documents contain the query terms
                query_terms_set = set(query_tokens)
                matching_docs = []
                for i, doc_tokens in enumerate(self.content_documents):
                    if query_terms_set.intersection(set(doc_tokens)):
                        # Found documents that contain the query terms, give them a small positive score
                        node = self.content_node_map[i]
                        matching_docs.append((node, 0.001))

                if matching_docs:
                    # Sort by node_id for consistent ordering when scores are equal
                    matching_docs.sort(key=lambda x: x[0].node_id)
                    return matching_docs[:top_k]
                else:
                    return []

            # Calculate threshold based on score distribution
            if max_score > 0:
                # For positive scores, require at least 5% of the max score (more lenient than metadata)
                threshold = max(0.001, max_score * 0.05)
            else:
                # For negative scores, use a more lenient approach for content search
                score_range = max_score - min_score
                if score_range > 0:
                    threshold = min_score + (score_range * 0.2)  # Bottom 20% threshold
                else:
                    threshold = max_score

            # Create (node, score) pairs and sort by score
            results = []
            for i, score in enumerate(scores):
                score_val = float(score)
                if score_val >= threshold:
                    node = self.content_node_map[i]
                    # Normalize scores to be positive for consistency
                    if max_score <= 0:
                        normalized_score = score_val - min_score + 0.001
                    else:
                        normalized_score = max(score_val, 0.001)
                    results.append((node, normalized_score))

            # Sort by score (descending) and return top_k
            results.sort(key=lambda x: x[1], reverse=True)
            return results[:top_k]

        except Exception as e:
            print(f"Warning: BM25 content search failed: {e}")
            return []

    def search(
        self, query: str, search_type: str = "all", top_k: int = 10
    ) -> List[Tuple[Node, float]]:
        """
        Unified search interface that can search across all levels.

        Args:
            query: The search query
            search_type: One of 'id', 'name', 'metadata', 'content', or 'all'
            top_k: Maximum number of results to return

        Returns:
            List of (node, score) tuples sorted by relevance
        """
        if search_type == "id":
            node = self.search_by_id(query)
            return [(node, 1.0)] if node else []

        elif search_type == "name":
            nodes = self.search_by_name(query)
            return [(node, 1.0) for node in nodes[:top_k]]

        elif search_type == "metadata":
            return self.search_by_metadata(query, top_k)

        elif search_type == "content":
            return self.search_by_content(query, top_k)

        elif search_type == "all":
            # Try exact matches first (Level 1 & 2)
            results = []
            seen_nodes = set()

            # Check ID match (Level 1) - highest priority
            node = self.search_by_id(query)
            if node:
                results.append((node, 1.0))
                seen_nodes.add(node.node_id)

            # Check name matches (Level 2) - high priority
            name_matches = self.search_by_name(query)
            for node in name_matches:
                if node.node_id not in seen_nodes:
                    results.append((node, 0.9))
                    seen_nodes.add(node.node_id)

            # If we don't have enough results, try metadata fuzzy search (Level 3)
            if len(results) < top_k:
                remaining_slots = top_k - len(results)
                metadata_matches = self.search_by_metadata(
                    query, remaining_slots * 2
                )  # Get extra to filter

                for node, score in metadata_matches:
                    if node.node_id not in seen_nodes and len(results) < top_k:
                        # Scale metadata scores to be lower than exact matches
                        scaled_score = score * 0.8  # Max 0.8 for metadata matches
                        results.append((node, scaled_score))
                        seen_nodes.add(node.node_id)

            # If we still don't have enough results, try content search (Level 4)
            if len(results) < top_k:
                remaining_slots = top_k - len(results)
                content_matches = self.search_by_content(
                    query, remaining_slots * 2
                )  # Get extra to filter

                for node, score in content_matches:
                    if node.node_id not in seen_nodes and len(results) < top_k:
                        # Scale content scores to be lower than metadata matches
                        scaled_score = score * 0.6  # Max 0.6 for content matches
                        results.append((node, scaled_score))
                        seen_nodes.add(node.node_id)

            # Sort by score and return top_k
            results.sort(key=lambda x: x[1], reverse=True)
            return results[:top_k]

        else:
            raise ValueError(f"Invalid search_type: {search_type}")

    def set_project_id(self, project_id: str) -> None:
        """Set the project identifier for this index."""
        self.project_id = project_id

    def get_project_id(self) -> Optional[str]:
        """Get the project identifier for this index."""
        return self.project_id

    def get_stats(self) -> Dict[str, Any]:
        """Get statistics about this index."""
        return {
            "project_id": self.project_id,
            "node_count": len(self.id_index),
            "unique_names": len(self.name_index),
            "metadata_documents": len(self.metadata_documents),
            "content_documents": len(self.content_documents),
            "index_levels_implemented": [
                "Level 1: ID Index",
                "Level 2: Name Index",
                "Level 3: BM25 Metadata Index",
                "Level 4: BM25 Content Index",
            ],
            "bm25_metadata_ready": self.metadata_bm25 is not None,
            "bm25_content_ready": self.content_bm25 is not None,
        }

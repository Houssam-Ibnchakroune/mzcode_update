#!/usr/bin/env python3
"""
Cache Manager for AI Enrichment
Handles storage and retrieval of AI-generated summaries to avoid redundant LLM API calls.
"""

import os
import json
import hashlib
from datetime import datetime
from typing import Dict, Any, Optional, List, Tuple
from pathlib import Path

from metazcode.sdk.models.graph import Node


class SummaryCache:
    """
    Manages caching of AI-generated summaries to optimize performance and reduce costs.

    Features:
    - Content-based hashing to detect changes
    - Timestamp tracking for cache validation
    - Cost and performance metrics
    - Incremental updates (only regenerate changed operations)
    """

    def __init__(self, project_path: str):
        """
        Initialize cache manager for a specific project.

        Args:
            project_path: Path to the SSIS project directory
        """
        self.project_path = Path(project_path)
        self.cache_dir = self.project_path / ".metazcode_cache"
        self.cache_file = self.cache_dir / "summaries.json"
        self.metadata_file = self.cache_dir / "cache_metadata.json"

        # Ensure cache directory exists
        self.cache_dir.mkdir(exist_ok=True)

        # Load existing cache
        self._cache: Dict[str, Dict[str, Any]] = self._load_cache()
        self._metadata: Dict[str, Any] = self._load_metadata()

    def _load_cache(self) -> Dict[str, Dict[str, Any]]:
        """Load cache from disk."""
        if self.cache_file.exists():
            try:
                with open(self.cache_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except (json.JSONDecodeError, FileNotFoundError):
                return {}
        return {}

    def _load_metadata(self) -> Dict[str, Any]:
        """Load cache metadata from disk."""
        if self.metadata_file.exists():
            try:
                with open(self.metadata_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except (json.JSONDecodeError, FileNotFoundError):
                return self._create_default_metadata()
        return self._create_default_metadata()

    def _create_default_metadata(self) -> Dict[str, Any]:
        """Create default metadata structure."""
        return {
            "created_at": datetime.now().isoformat(),
            "last_updated": datetime.now().isoformat(),
            "total_summaries": 0,
            "total_api_calls": 0,
            "total_cost": 0.0,
            "cache_hits": 0,
            "cache_misses": 0,
            "version": "1.0",
        }

    def _save_cache(self) -> None:
        """Save cache to disk."""
        with open(self.cache_file, "w", encoding="utf-8") as f:
            json.dump(self._cache, f, indent=2, ensure_ascii=False)

    def _save_metadata(self) -> None:
        """Save metadata to disk."""
        self._metadata["last_updated"] = datetime.now().isoformat()
        with open(self.metadata_file, "w", encoding="utf-8") as f:
            json.dump(self._metadata, f, indent=2, ensure_ascii=False)

    def _compute_content_hash(self, node: Node) -> str:
        """
        Compute content hash for a node to detect changes.

        Args:
            node: The node to hash

        Returns:
            SHA256 hash of node content
        """
        # Create deterministic string representation of node content
        content_parts = [
            node.node_id,
            node.node_type,
            node.name,
            json.dumps(node.properties, sort_keys=True),
            json.dumps(node.context, sort_keys=True),
        ]

        content_string = "||".join(str(part) for part in content_parts)
        return hashlib.sha256(content_string.encode("utf-8")).hexdigest()

    def get_cached_summary(self, node: Node) -> Optional[Dict[str, Any]]:
        """
        Get cached summary for a node if it exists and is valid.

        Args:
            node: The node to get summary for

        Returns:
            Cached summary data or None if not found/invalid
        """
        node_id = node.node_id

        if node_id not in self._cache:
            self._metadata["cache_misses"] += 1
            return None

        cached_entry = self._cache[node_id]
        current_hash = self._compute_content_hash(node)

        # Check if content has changed
        if cached_entry.get("content_hash") != current_hash:
            self._metadata["cache_misses"] += 1
            # Remove stale cache entry
            del self._cache[node_id]
            return None

        # Cache hit!
        self._metadata["cache_hits"] += 1
        return cached_entry

    def store_summary(
        self, node: Node, summary_data: Dict[str, Any], api_cost: float = 0.0
    ) -> None:
        """
        Store a generated summary in cache.

        Args:
            node: The node that was summarized
            summary_data: The generated summary data
            api_cost: Cost of the API call
        """
        node_id = node.node_id
        content_hash = self._compute_content_hash(node)

        cache_entry = {
            "content_hash": content_hash,
            "generated_at": datetime.now().isoformat(),
            "summary_data": summary_data,
            "api_cost": api_cost,
            "node_type": node.node_type,
            "node_name": node.name,
        }

        # Update cache
        self._cache[node_id] = cache_entry

        # Update metadata
        self._metadata["total_summaries"] += 1
        self._metadata["total_api_calls"] += 1
        self._metadata["total_cost"] += api_cost

        # Save to disk
        self._save_cache()
        self._save_metadata()

    def get_cache_stats(self) -> Dict[str, Any]:
        """
        Get cache performance statistics.

        Returns:
            Dictionary with cache statistics
        """
        total_requests = self._metadata["cache_hits"] + self._metadata["cache_misses"]
        hit_rate = (
            (self._metadata["cache_hits"] / total_requests * 100)
            if total_requests > 0
            else 0
        )

        return {
            "cache_entries": len(self._cache),
            "total_requests": total_requests,
            "cache_hits": self._metadata["cache_hits"],
            "cache_misses": self._metadata["cache_misses"],
            "hit_rate_percent": round(hit_rate, 2),
            "total_api_calls": self._metadata["total_api_calls"],
            "total_cost": round(self._metadata["total_cost"], 4),
            "estimated_savings": round(
                self._metadata["total_cost"] * (hit_rate / 100), 4
            ),
            "cache_size_mb": round(self.cache_file.stat().st_size / 1024 / 1024, 2)
            if self.cache_file.exists()
            else 0,
            "last_updated": self._metadata["last_updated"],
        }

    def clear_cache(self) -> int:
        """
        Clear all cached summaries.

        Returns:
            Number of entries cleared
        """
        entries_cleared = len(self._cache)
        self._cache.clear()
        self._metadata = self._create_default_metadata()

        # Remove cache files
        if self.cache_file.exists():
            self.cache_file.unlink()
        if self.metadata_file.exists():
            self.metadata_file.unlink()

        return entries_cleared

    def invalidate_node(self, node_id: str) -> bool:
        """
        Invalidate cache for a specific node.

        Args:
            node_id: ID of the node to invalidate

        Returns:
            True if entry was found and removed, False otherwise
        """
        if node_id in self._cache:
            del self._cache[node_id]
            self._save_cache()
            return True
        return False

    def get_stale_entries(self, max_age_days: int = 30) -> List[str]:
        """
        Get list of cache entries older than specified age.

        Args:
            max_age_days: Maximum age in days

        Returns:
            List of node IDs with stale cache entries
        """
        stale_entries = []
        cutoff_date = datetime.now().timestamp() - (max_age_days * 24 * 60 * 60)

        for node_id, entry in self._cache.items():
            try:
                entry_date = datetime.fromisoformat(entry["generated_at"]).timestamp()
                if entry_date < cutoff_date:
                    stale_entries.append(node_id)
            except (ValueError, KeyError):
                # Invalid date format, consider stale
                stale_entries.append(node_id)

        return stale_entries

    def cleanup_stale_entries(self, max_age_days: int = 30) -> int:
        """
        Remove stale cache entries.

        Args:
            max_age_days: Maximum age in days

        Returns:
            Number of entries removed
        """
        stale_entries = self.get_stale_entries(max_age_days)

        for node_id in stale_entries:
            del self._cache[node_id]

        if stale_entries:
            self._save_cache()

        return len(stale_entries)

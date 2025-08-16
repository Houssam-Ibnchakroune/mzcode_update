"""
Integration module for coordinating different SDK components.

This module provides utilities for combining ingestion, indexing, and other
SDK capabilities while maintaining clean separation between components.
"""

from .index_integration import IndexIntegration

__all__ = ["IndexIntegration"]
"""
Caching Infrastructure Module

This module provides intelligent caching capabilities for AI-generated summaries
to optimize performance and reduce operational costs. Features include content-based
invalidation, performance metrics, and zero-configuration setup.
"""

from .cache_manager import SummaryCache

__all__ = [
    "SummaryCache",
]

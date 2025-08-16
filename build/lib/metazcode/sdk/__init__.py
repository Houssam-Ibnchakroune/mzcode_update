# -*- coding: utf-8 -*-
"""
MetazCode SDK - Enterprise SSIS Analysis and Metadata Extraction

A comprehensive SDK for analyzing SSIS/ETL pipelines with business logic extraction,
cross-package dependency analysis, and hierarchical indexing capabilities.

Core Architecture:
- ingestion/: SSIS parsing and metadata extraction
- analysis/: Cross-package dependency analysis
- graph/: Universal metadata graph operations
- indexing/: Hierarchical search and indexing
- caching/: Performance optimization
- context/: Context collection utilities
"""

# Quality assurance exports
from .quality import SummaryValidator, ValidationLevel, QualityFlag, BusinessDomain

# Caching infrastructure exports
from .caching import SummaryCache

# Context processing exports
from .context import ContextCollector, PromptFactory

__all__ = [
    "SummaryValidator",
    "ValidationLevel", 
    "QualityFlag",
    "BusinessDomain",
    "SummaryCache",
    "ContextCollector",
    "PromptFactory",
]

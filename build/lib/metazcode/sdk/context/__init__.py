"""
Context Processing Module

This module handles context collection, enrichment, and processing for AI operations.
It provides utilities for gathering operational context, prompt generation, and
context-aware processing.
"""

from .context_collector import ContextCollector
from .prompt_factory import PromptFactory

__all__ = [
    "ContextCollector",
    "PromptFactory",
]

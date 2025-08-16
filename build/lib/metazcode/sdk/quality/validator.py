"""
Task 4.1: Summary Validation and Quality Metrics for Enterprise Adoption

This module provides comprehensive quality validation for AI-generated summaries,
including automatic quality checks, business context validation, confidence scoring,
and manual review workflow capabilities.
"""

import re
import time
import json
import logging
from enum import Enum
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Set, Tuple, Any
from pathlib import Path

from ..models.canonical_types import NodeType


class ValidationLevel(Enum):
    """Validation strictness levels for different environments."""

    DEVELOPMENT = "development"  # Relaxed validation for dev/testing
    STAGING = "staging"  # Moderate validation for pre-production
    PRODUCTION = "production"  # Strict validation for enterprise use


class QualityFlag(Enum):
    """Quality flags for summary validation."""

    PASSED = "passed"
    WARNING = "warning"
    FAILED = "failed"
    NEEDS_REVIEW = "needs_review"


class BusinessDomain(Enum):
    """Business domains for context validation."""

    SALES = "sales"
    FINANCE = "finance"
    HR = "human_resources"
    OPERATIONS = "operations"
    WAREHOUSE = "warehouse"
    ANALYTICS = "analytics"
    COMPLIANCE = "compliance"
    GENERAL = "general"


@dataclass
class ValidationRule:
    """Configuration for a validation rule."""

    name: str
    description: str
    enabled: bool = True
    min_confidence: float = 0.0
    validation_level: ValidationLevel = ValidationLevel.DEVELOPMENT


@dataclass
class QualityMetrics:
    """Comprehensive quality metrics for a summary."""

    # Basic metrics
    length_score: float  # 0-1 score based on optimal length
    readability_score: float  # 0-1 score based on readability
    business_context_score: float  # 0-1 score based on business keywords
    technical_jargon_penalty: float  # 0-1 penalty for excessive technical terms

    # Advanced metrics
    confidence_score: float  # 0-1 overall confidence in summary quality
    domain_relevance_score: float  # 0-1 score for domain-specific relevance
    completeness_score: float  # 0-1 score based on context coverage

    # Validation flags
    quality_flag: QualityFlag
    validation_messages: List[str]
    requires_review: bool

    # Context metrics
    context_richness: float  # 0-1 score based on available context
    source_confidence: float  # 0-1 confidence in source data quality


@dataclass
class ReviewRecord:
    """Record of manual review and feedback."""

    summary_id: str
    reviewer_id: str
    review_timestamp: float
    original_summary: str
    corrected_summary: Optional[str]
    quality_rating: int  # 1-5 scale
    feedback_notes: str
    approved: bool
    review_flags: List[str]


class SummaryValidator:
    """
    Enterprise-grade summary validation system.

    Provides comprehensive quality checks, business context validation,
    confidence scoring, and manual review workflow capabilities.
    """

    def __init__(
        self,
        validation_level: ValidationLevel = ValidationLevel.DEVELOPMENT,
        project_path: str = "",
        config_path: Optional[str] = None,
    ):
        """
        Initialize the summary validator.

        Args:
            validation_level: Strictness level for validation
            project_path: Path to project for storing review data
            config_path: Path to custom validation configuration
        """
        self.validation_level = validation_level
        self.project_path = Path(project_path) if project_path else None
        self.logger = logging.getLogger(__name__)

        # Initialize validation rules and keywords
        self._load_validation_config(config_path)
        self._setup_review_storage()

        # Performance tracking
        self.validation_stats = {
            "total_validations": 0,
            "passed_validations": 0,
            "failed_validations": 0,
            "reviews_required": 0,
            "avg_confidence_score": 0.0,
        }

    def _load_validation_config(self, config_path: Optional[str]) -> None:
        """Load validation configuration and keyword sets."""

        # Business keywords by domain
        self.business_keywords = {
            BusinessDomain.SALES: {
                "revenue",
                "sales",
                "customer",
                "order",
                "transaction",
                "purchase",
                "billing",
                "invoice",
                "payment",
                "conversion",
                "lead",
                "opportunity",
                "pipeline",
                "forecast",
                "quota",
                "commission",
                "pricing",
                "product",
            },
            BusinessDomain.FINANCE: {
                "financial",
                "accounting",
                "budget",
                "cost",
                "expense",
                "profit",
                "margin",
                "cash",
                "flow",
                "balance",
                "ledger",
                "journal",
                "audit",
                "tax",
                "compliance",
                "reporting",
                "statement",
                "analysis",
            },
            BusinessDomain.HR: {
                "employee",
                "staff",
                "workforce",
                "personnel",
                "payroll",
                "benefits",
                "performance",
                "evaluation",
                "training",
                "recruitment",
                "hire",
                "department",
                "organization",
                "management",
                "role",
                "position",
            },
            BusinessDomain.OPERATIONS: {
                "process",
                "workflow",
                "operation",
                "efficiency",
                "productivity",
                "quality",
                "performance",
                "monitoring",
                "optimization",
                "automation",
                "scheduling",
                "resource",
                "capacity",
                "throughput",
                "delivery",
            },
            BusinessDomain.WAREHOUSE: {
                "data",
                "warehouse",
                "dimensional",
                "fact",
                "dimension",
                "staging",
                "etl",
                "extract",
                "transform",
                "load",
                "integration",
                "consolidation",
                "reporting",
                "analytics",
                "business intelligence",
                "dashboard",
            },
            BusinessDomain.ANALYTICS: {
                "analysis",
                "analytics",
                "insights",
                "trends",
                "patterns",
                "metrics",
                "kpi",
                "dashboard",
                "reporting",
                "visualization",
                "intelligence",
                "forecasting",
                "prediction",
                "modeling",
                "statistics",
                "data mining",
            },
        }

        # Technical jargon that should be minimized
        self.technical_jargon = {
            "dll",
            "api",
            "sql",
            "jdbc",
            "odbc",
            "xml",
            "json",
            "csv",
            "etl",
            "ssis",
            "dtsx",
            "oledb",
            "ado",
            "connection",
            "dataset",
            "recordset",
            "stored procedure",
            "function",
            "trigger",
            "index",
            "primary key",
            "foreign key",
            "constraint",
            "schema",
            "metadata",
            "configuration",
        }

        # Validation rules configuration
        self.validation_rules = {
            "length_check": ValidationRule(
                name="Length Check",
                description="Validates summary length is within optimal range",
                enabled=True,
                min_confidence=0.7,
            ),
            "business_context": ValidationRule(
                name="Business Context",
                description="Validates presence of business-relevant keywords",
                enabled=True,
                min_confidence=0.6,
            ),
            "technical_jargon": ValidationRule(
                name="Technical Jargon Penalty",
                description="Penalizes excessive technical terminology",
                enabled=True,
                min_confidence=0.5,
            ),
            "readability": ValidationRule(
                name="Readability Check",
                description="Validates summary readability for business users",
                enabled=True,
                min_confidence=0.6,
            ),
            "completeness": ValidationRule(
                name="Completeness Check",
                description="Validates summary covers key operation aspects",
                enabled=True,
                min_confidence=0.7,
            ),
        }

        # Load custom config if provided
        if config_path and Path(config_path).exists():
            self._load_custom_config(config_path)

    def _setup_review_storage(self) -> None:
        """Setup storage for manual review records."""
        if self.project_path:
            self.review_dir = self.project_path / ".metazcode" / "reviews"
            self.review_dir.mkdir(parents=True, exist_ok=True)
            self.review_log_path = self.review_dir / "review_log.jsonl"

    def validate_summary(
        self,
        summary_text: str,
        operation_id: str,
        context_data: Dict[str, Any],
        operation_type: str = "operation",
    ) -> QualityMetrics:
        """
        Perform comprehensive validation of a generated summary.

        Args:
            summary_text: The AI-generated summary to validate
            operation_id: ID of the operation being summarized
            context_data: Context data used for summary generation
            operation_type: Type of operation (operation, pipeline, etc.)

        Returns:
            QualityMetrics with comprehensive validation results
        """
        self.validation_stats["total_validations"] += 1
        validation_messages = []

        # 1. Length validation
        length_score, length_msg = self._validate_length(summary_text)
        if length_msg:
            validation_messages.append(length_msg)

        # 2. Business context validation
        business_score, domain, business_msg = self._validate_business_context(
            summary_text, context_data
        )
        if business_msg:
            validation_messages.append(business_msg)

        # 3. Technical jargon penalty
        jargon_penalty, jargon_msg = self._calculate_jargon_penalty(summary_text)
        if jargon_msg:
            validation_messages.append(jargon_msg)

        # 4. Readability assessment
        readability_score, readability_msg = self._assess_readability(summary_text)
        if readability_msg:
            validation_messages.append(readability_msg)

        # 5. Context-based completeness
        completeness_score, completeness_msg = self._assess_completeness(
            summary_text, context_data
        )
        if completeness_msg:
            validation_messages.append(completeness_msg)

        # 6. Context richness assessment
        context_richness = self._assess_context_richness(context_data)

        # 7. Source confidence
        source_confidence = self._assess_source_confidence(context_data)

        # 8. Calculate overall confidence score
        confidence_score = self._calculate_confidence_score(
            length_score,
            business_score,
            jargon_penalty,
            readability_score,
            completeness_score,
            context_richness,
            source_confidence,
        )

        # 9. Domain relevance
        domain_relevance_score = business_score  # Currently same as business score

        # 10. Determine quality flag and review requirement
        quality_flag, requires_review = self._determine_quality_flag(
            confidence_score, validation_messages
        )

        # Update statistics
        if quality_flag in [QualityFlag.PASSED, QualityFlag.WARNING]:
            self.validation_stats["passed_validations"] += 1
        else:
            self.validation_stats["failed_validations"] += 1

        if requires_review:
            self.validation_stats["reviews_required"] += 1

        # Update average confidence
        total_vals = self.validation_stats["total_validations"]
        current_avg = self.validation_stats["avg_confidence_score"]
        new_avg = ((current_avg * (total_vals - 1)) + confidence_score) / total_vals
        self.validation_stats["avg_confidence_score"] = new_avg

        return QualityMetrics(
            length_score=length_score,
            readability_score=readability_score,
            business_context_score=business_score,
            technical_jargon_penalty=jargon_penalty,
            confidence_score=confidence_score,
            domain_relevance_score=domain_relevance_score,
            completeness_score=completeness_score,
            quality_flag=quality_flag,
            validation_messages=validation_messages,
            requires_review=requires_review,
            context_richness=context_richness,
            source_confidence=source_confidence,
        )

    def _validate_length(self, summary_text: str) -> Tuple[float, Optional[str]]:
        """Validate summary length is within optimal range."""
        words = len(summary_text.split())
        chars = len(summary_text)

        # Optimal ranges based on validation level
        if self.validation_level == ValidationLevel.PRODUCTION:
            min_words, max_words = 15, 100
            min_chars, max_chars = 75, 500
        elif self.validation_level == ValidationLevel.STAGING:
            min_words, max_words = 10, 120
            min_chars, max_chars = 50, 600
        else:  # DEVELOPMENT
            min_words, max_words = 5, 150
            min_chars, max_chars = 25, 750

        # Calculate score
        if min_words <= words <= max_words and min_chars <= chars <= max_chars:
            score = 1.0
            message = None
        elif words < min_words or chars < min_chars:
            score = max(0.0, min(words / min_words, chars / min_chars))
            message = f"Summary too short: {words} words, {chars} characters"
        else:
            score = max(0.0, min(max_words / words, max_chars / chars))
            message = f"Summary too long: {words} words, {chars} characters"

        return score, message

    def _validate_business_context(
        self, summary_text: str, context_data: Dict[str, Any]
    ) -> Tuple[float, BusinessDomain, Optional[str]]:
        """Validate presence of business-relevant keywords and context."""
        summary_lower = summary_text.lower()

        # Detect primary business domain
        domain_scores = {}
        for domain, keywords in self.business_keywords.items():
            score = sum(1 for keyword in keywords if keyword in summary_lower)
            domain_scores[domain] = score / len(keywords)

        primary_domain = max(domain_scores, key=domain_scores.get)
        max_score = domain_scores[primary_domain]

        # Check for business value language
        business_value_phrases = [
            "business",
            "enables",
            "supports",
            "critical",
            "important",
            "decision",
            "analysis",
            "reporting",
            "tracking",
            "monitoring",
            "optimization",
            "efficiency",
            "performance",
            "value",
            "insights",
        ]

        business_value_score = sum(
            1 for phrase in business_value_phrases if phrase in summary_lower
        ) / len(business_value_phrases)

        # Combined score
        combined_score = (max_score * 0.7) + (business_value_score * 0.3)

        # Generate message if score is low
        message = None
        if combined_score < 0.3:
            message = f"Low business context relevance (score: {combined_score:.2f})"
        elif combined_score < 0.5:
            message = (
                f"Moderate business context relevance (score: {combined_score:.2f})"
            )

        return combined_score, primary_domain, message

    def _calculate_jargon_penalty(
        self, summary_text: str
    ) -> Tuple[float, Optional[str]]:
        """Calculate penalty for excessive technical jargon."""
        summary_lower = summary_text.lower()
        words = summary_text.split()

        jargon_count = sum(
            1 for jargon in self.technical_jargon if jargon in summary_lower
        )
        jargon_ratio = jargon_count / len(words) if words else 0

        # Calculate penalty (0 = no penalty, 1 = maximum penalty)
        if jargon_ratio < 0.1:
            penalty = 0.0
            message = None
        elif jargon_ratio < 0.2:
            penalty = 0.3
            message = f"Moderate technical jargon detected ({jargon_count} terms)"
        else:
            penalty = min(1.0, jargon_ratio * 2)
            message = f"High technical jargon detected ({jargon_count} terms)"

        return penalty, message

    def _assess_readability(self, summary_text: str) -> Tuple[float, Optional[str]]:
        """Assess readability for business users."""
        words = summary_text.split()
        sentences = (
            summary_text.count(".") + summary_text.count("!") + summary_text.count("?")
        )

        if not words or not sentences:
            return 0.0, "Empty or invalid summary"

        # Simple readability metrics
        avg_words_per_sentence = len(words) / sentences
        long_words = sum(1 for word in words if len(word) > 6)
        long_word_ratio = long_words / len(words)

        # Calculate readability score (higher is better)
        sentence_score = (
            1.0
            if avg_words_per_sentence <= 20
            else max(0.0, 20 / avg_words_per_sentence)
        )
        word_score = (
            1.0
            if long_word_ratio <= 0.3
            else max(0.0, (0.3 - long_word_ratio) / 0.3 + 1.0)
        )

        readability_score = (sentence_score + word_score) / 2

        message = None
        if readability_score < 0.5:
            message = f"Low readability (avg {avg_words_per_sentence:.1f} words/sentence, {long_word_ratio:.1%} long words)"

        return readability_score, message

    def _assess_completeness(
        self, summary_text: str, context_data: Dict[str, Any]
    ) -> Tuple[float, Optional[str]]:
        """Assess how completely the summary covers the operation context."""
        summary_lower = summary_text.lower()

        # Key aspects that should be covered
        coverage_aspects = []

        # Check if data sources/destinations are mentioned
        sources = context_data.get("source_connections", [])
        destinations = context_data.get("destination_connections", [])

        if sources and ("source" in summary_lower or "from" in summary_lower):
            coverage_aspects.append("sources")

        if destinations and ("destination" in summary_lower or "to" in summary_lower):
            coverage_aspects.append("destinations")

        # Check if transformation is mentioned
        transformations = context_data.get("transformation_summary", "")
        if transformations and transformations != "No transformations":
            if any(
                word in summary_lower
                for word in ["transform", "convert", "process", "change"]
            ):
                coverage_aspects.append("transformations")

        # Check if business purpose is mentioned
        if any(
            word in summary_lower
            for word in ["purpose", "business", "enables", "supports"]
        ):
            coverage_aspects.append("business_purpose")

        # Calculate completeness score
        max_aspects = 4  # sources, destinations, transformations, business_purpose
        coverage_score = len(coverage_aspects) / max_aspects

        message = None
        if coverage_score < 0.5:
            missing = max_aspects - len(coverage_aspects)
            message = f"Incomplete coverage: missing {missing} key aspects"

        return coverage_score, message

    def _assess_context_richness(self, context_data: Dict[str, Any]) -> float:
        """Assess the richness of available context data."""
        richness_factors = []

        # Check for various context elements
        if context_data.get("source_connections"):
            richness_factors.append(1.0)
        else:
            richness_factors.append(0.0)

        if context_data.get("destination_connections"):
            richness_factors.append(1.0)
        else:
            richness_factors.append(0.0)

        transformations = context_data.get("transformation_summary", "")
        if transformations and transformations != "No transformations":
            richness_factors.append(1.0)
        else:
            richness_factors.append(0.5)

        if context_data.get("parent_pipeline"):
            richness_factors.append(1.0)
        else:
            richness_factors.append(0.3)

        return (
            sum(richness_factors) / len(richness_factors) if richness_factors else 0.0
        )

    def _assess_source_confidence(self, context_data: Dict[str, Any]) -> float:
        """Assess confidence in the source data quality."""
        confidence_factors = []

        # Check data completeness
        if context_data.get("operation_name"):
            confidence_factors.append(1.0)
        else:
            confidence_factors.append(0.0)

        # Check for detailed operation info
        if context_data.get("operation_details"):
            confidence_factors.append(1.0)
        else:
            confidence_factors.append(0.5)

        # Check for connection information
        total_connections = len(context_data.get("source_connections", [])) + len(
            context_data.get("destination_connections", [])
        )
        if total_connections > 0:
            confidence_factors.append(min(1.0, total_connections / 2))
        else:
            confidence_factors.append(0.0)

        return (
            sum(confidence_factors) / len(confidence_factors)
            if confidence_factors
            else 0.0
        )

    def _calculate_confidence_score(
        self,
        length_score: float,
        business_score: float,
        jargon_penalty: float,
        readability_score: float,
        completeness_score: float,
        context_richness: float,
        source_confidence: float,
    ) -> float:
        """Calculate overall confidence score using weighted factors."""

        # Weights based on validation level
        if self.validation_level == ValidationLevel.PRODUCTION:
            weights = {
                "length": 0.15,
                "business": 0.25,
                "jargon": 0.20,
                "readability": 0.20,
                "completeness": 0.20,
            }
        elif self.validation_level == ValidationLevel.STAGING:
            weights = {
                "length": 0.15,
                "business": 0.20,
                "jargon": 0.15,
                "readability": 0.25,
                "completeness": 0.25,
            }
        else:  # DEVELOPMENT
            weights = {
                "length": 0.20,
                "business": 0.15,
                "jargon": 0.10,
                "readability": 0.25,
                "completeness": 0.30,
            }

        # Calculate weighted score
        weighted_score = (
            length_score * weights["length"]
            + business_score * weights["business"]
            + (1.0 - jargon_penalty) * weights["jargon"]
            + readability_score * weights["readability"]
            + completeness_score * weights["completeness"]
        )

        # Apply context and source confidence modifiers
        context_modifier = 0.8 + (context_richness * 0.2)  # 0.8 to 1.0
        source_modifier = 0.9 + (source_confidence * 0.1)  # 0.9 to 1.0

        final_score = weighted_score * context_modifier * source_modifier

        return min(1.0, max(0.0, final_score))

    def _determine_quality_flag(
        self, confidence_score: float, validation_messages: List[str]
    ) -> Tuple[QualityFlag, bool]:
        """Determine quality flag and review requirement."""

        # Thresholds based on validation level
        if self.validation_level == ValidationLevel.PRODUCTION:
            excellent_threshold = 0.8
            good_threshold = 0.6
            acceptable_threshold = 0.4
        elif self.validation_level == ValidationLevel.STAGING:
            excellent_threshold = 0.7
            good_threshold = 0.5
            acceptable_threshold = 0.3
        else:  # DEVELOPMENT
            excellent_threshold = 0.6
            good_threshold = 0.4
            acceptable_threshold = 0.2

        requires_review = False

        if confidence_score >= excellent_threshold:
            quality_flag = QualityFlag.PASSED
        elif confidence_score >= good_threshold:
            quality_flag = QualityFlag.WARNING
            requires_review = self.validation_level == ValidationLevel.PRODUCTION
        elif confidence_score >= acceptable_threshold:
            quality_flag = QualityFlag.NEEDS_REVIEW
            requires_review = True
        else:
            quality_flag = QualityFlag.FAILED
            requires_review = True

        # Force review for critical issues
        critical_keywords = ["too short", "too long", "low business context"]
        if any(
            keyword in msg.lower()
            for msg in validation_messages
            for keyword in critical_keywords
        ):
            requires_review = True
            if quality_flag == QualityFlag.PASSED:
                quality_flag = QualityFlag.WARNING

        return quality_flag, requires_review

    def create_review_record(
        self,
        summary_id: str,
        reviewer_id: str,
        original_summary: str,
        corrected_summary: Optional[str] = None,
        quality_rating: int = 3,
        feedback_notes: str = "",
        approved: bool = False,
        review_flags: Optional[List[str]] = None,
    ) -> ReviewRecord:
        """Create a manual review record."""

        record = ReviewRecord(
            summary_id=summary_id,
            reviewer_id=reviewer_id,
            review_timestamp=time.time(),
            original_summary=original_summary,
            corrected_summary=corrected_summary,
            quality_rating=quality_rating,
            feedback_notes=feedback_notes,
            approved=approved,
            review_flags=review_flags or [],
        )

        # Store review record
        self._store_review_record(record)

        return record

    def _store_review_record(self, record: ReviewRecord) -> None:
        """Store review record to persistent storage."""
        if not self.project_path or not self.review_log_path:
            return

        try:
            with open(self.review_log_path, "a") as f:
                f.write(json.dumps(asdict(record)) + "\n")
        except Exception as e:
            self.logger.error(f"Failed to store review record: {e}")

    def get_review_statistics(self) -> Dict[str, Any]:
        """Get statistics about manual reviews."""
        if not self.project_path or not self.review_log_path.exists():
            return {"total_reviews": 0}

        reviews = []
        try:
            with open(self.review_log_path, "r") as f:
                for line in f:
                    reviews.append(json.loads(line.strip()))
        except Exception as e:
            self.logger.error(f"Failed to read review records: {e}")
            return {"error": str(e)}

        if not reviews:
            return {"total_reviews": 0}

        total_reviews = len(reviews)
        approved_reviews = sum(1 for r in reviews if r["approved"])
        avg_rating = sum(r["quality_rating"] for r in reviews) / total_reviews

        return {
            "total_reviews": total_reviews,
            "approved_reviews": approved_reviews,
            "approval_rate": approved_reviews / total_reviews,
            "average_quality_rating": avg_rating,
            "recent_reviews": reviews[-5:] if reviews else [],
        }

    def get_validation_statistics(self) -> Dict[str, Any]:
        """Get comprehensive validation statistics."""
        stats = self.validation_stats.copy()

        if stats["total_validations"] > 0:
            stats["pass_rate"] = (
                stats["passed_validations"] / stats["total_validations"]
            )
            stats["review_rate"] = (
                stats["reviews_required"] / stats["total_validations"]
            )
        else:
            stats["pass_rate"] = 0.0
            stats["review_rate"] = 0.0

        return stats

    def _load_custom_config(self, config_path: str) -> None:
        """Load custom validation configuration from file."""
        try:
            with open(config_path, "r") as f:
                config = json.load(f)

            # Update business keywords
            if "business_keywords" in config:
                for domain_name, keywords in config["business_keywords"].items():
                    try:
                        domain = BusinessDomain(domain_name)
                        self.business_keywords[domain].update(keywords)
                    except ValueError:
                        self.logger.warning(f"Unknown business domain: {domain_name}")

            # Update technical jargon
            if "technical_jargon" in config:
                self.technical_jargon.update(config["technical_jargon"])

            # Update validation rules
            if "validation_rules" in config:
                for rule_name, rule_config in config["validation_rules"].items():
                    if rule_name in self.validation_rules:
                        for key, value in rule_config.items():
                            if hasattr(self.validation_rules[rule_name], key):
                                setattr(self.validation_rules[rule_name], key, value)

        except Exception as e:
            self.logger.error(f"Failed to load custom config: {e}")

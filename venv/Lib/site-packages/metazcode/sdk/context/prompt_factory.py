"""
Enhanced Prompt Factory for Context-Aware LLM Summaries

This module implements Task 1.2: Design Enhanced Prompt Template
- Creates context-aware prompt templates with placeholders for pipeline, sources, destinations, etc.
- Focuses on business purpose rather than technical details
- Keeps summaries concise (1-2 sentences) as originally planned
- Supports testing with different operation types

This is part of the optional LLM enrichment layer.
"""

from typing import Dict, List, Any, Optional, TYPE_CHECKING
from dataclasses import dataclass

if TYPE_CHECKING:
    from .summarizer import PipelineContext


@dataclass
class OperationContext:
    """Structured representation of operation context for prompt generation."""

    operation_name: str
    operation_type: str
    pipeline_name: str
    source_connections: List[str]
    destination_connections: List[str]
    transformation_summary: str
    business_domain: str = "data processing"


class PromptFactory:
    """
    Factory for generating context-aware prompts for LLM operation summaries.

    This class creates business-focused prompts that help LLMs understand the
    purpose and context of SSIS operations rather than just their technical details.
    """

    def __init__(self):
        """Initialize the prompt factory with predefined templates."""
        self.templates = {
            "business_summary": self._get_business_summary_template(),
            "technical_summary": self._get_technical_summary_template(),
            "context_analysis": self._get_context_analysis_template(),
        }

    def _get_business_summary_template(self) -> str:
        """
        Get the main business-focused summary template.

        This template focuses on business purpose and keeps to 1-2 sentences.
        """
        return """Based on the following SSIS operation context, provide a concise 1-2 sentence business summary that explains WHAT this operation accomplishes and WHY it's important for the business:

Operation: {operation_name}
Pipeline: {pipeline_name}
Data Sources: {sources}
Data Destinations: {destinations}
Transformations: {transformation_summary}

Focus on:
- Business purpose (what business need does this serve?)
- Data value (what insights or capabilities does this enable?)
- Avoid technical jargon - explain in business terms

Business Summary:"""

    def _get_technical_summary_template(self) -> str:
        """
        Get a technical summary template for detailed analysis.
        """
        return """Analyze this SSIS operation and provide a technical summary of its data processing approach:

Operation: {operation_name} (Type: {operation_type})
Pipeline: {pipeline_name}
Data Flow: {sources} → {transformation_summary} → {destinations}

Provide a brief technical summary focusing on:
- Data transformation approach
- Integration pattern used
- Performance or design considerations

Technical Summary:"""

    def _get_context_analysis_template(self) -> str:
        """
        Get a context analysis template for understanding operation relationships.
        """
        return """Analyze the context and relationships of this SSIS operation:

Operation: {operation_name}
Pipeline Context: {pipeline_name}
Upstream Systems: {sources}
Downstream Systems: {destinations}
Processing: {transformation_summary}

Analyze:
- How this operation fits in the overall data pipeline
- Dependencies and relationships with other systems
- Business impact if this operation fails

Context Analysis:"""

    def create_business_prompt(self, context: OperationContext) -> str:
        """
        Create a business-focused prompt for operation summary.

        Args:
            context: Structured operation context

        Returns:
            Formatted prompt ready for LLM
        """
        return self.templates["business_summary"].format(
            operation_name=context.operation_name,
            pipeline_name=context.pipeline_name,
            sources=self._format_connections(context.source_connections),
            destinations=self._format_connections(context.destination_connections),
            transformation_summary=context.transformation_summary
            or "No transformations",
        )

    def create_technical_prompt(self, context: OperationContext) -> str:
        """
        Create a technical-focused prompt for operation analysis.

        Args:
            context: Structured operation context

        Returns:
            Formatted prompt ready for LLM
        """
        return self.templates["technical_summary"].format(
            operation_name=context.operation_name,
            operation_type=context.operation_type,
            pipeline_name=context.pipeline_name,
            sources=self._format_connections(context.source_connections),
            destinations=self._format_connections(context.destination_connections),
            transformation_summary=context.transformation_summary
            or "No transformations",
        )

    def create_context_prompt(self, context: OperationContext) -> str:
        """
        Create a context analysis prompt for understanding operation relationships.

        Args:
            context: Structured operation context

        Returns:
            Formatted prompt ready for LLM
        """
        return self.templates["context_analysis"].format(
            operation_name=context.operation_name,
            pipeline_name=context.pipeline_name,
            sources=self._format_connections(context.source_connections),
            destinations=self._format_connections(context.destination_connections),
            transformation_summary=context.transformation_summary
            or "No transformations",
        )

    def create_domain_specific_prompt(
        self, context: OperationContext, domain_hints: Dict[str, str]
    ) -> str:
        """
        Create a domain-specific business prompt with additional context clues.

        Args:
            context: Structured operation context
            domain_hints: Additional domain-specific information (e.g., customer data, sales data)

        Returns:
            Enhanced business prompt with domain context
        """
        base_prompt = self.create_business_prompt(context)

        # Add domain-specific context
        domain_context = self._build_domain_context(context, domain_hints)
        if domain_context:
            enhanced_prompt = (
                base_prompt
                + f"\n\nDomain Context: {domain_context}\n\nBusiness Summary:"
            )
            return enhanced_prompt

        return base_prompt

    def _format_connections(self, connections: List[str]) -> str:
        """
        Format connection lists for readable prompt inclusion.

        Args:
            connections: List of connection names

        Returns:
            Formatted string representation
        """
        if not connections:
            return "None"

        if len(connections) == 1:
            return connections[0]

        if len(connections) <= 3:
            return ", ".join(connections)

        # For many connections, show first few and count
        return f"{', '.join(connections[:3])} and {len(connections) - 3} others"

    def _build_domain_context(
        self, context: OperationContext, domain_hints: Dict[str, str]
    ) -> str:
        """
        Build domain-specific context hints for enhanced prompts.

        Args:
            context: Operation context
            domain_hints: Domain-specific hints and patterns

        Returns:
            Domain context string
        """
        domain_parts = []

        # Detect business domains from connection names and data
        detected_domains = self._detect_business_domains(context)
        if detected_domains:
            domain_parts.append(f"Business domains: {', '.join(detected_domains)}")

        # Add custom domain hints
        for key, value in domain_hints.items():
            if value:
                domain_parts.append(f"{key}: {value}")

        return "; ".join(domain_parts)

    def _detect_business_domains(self, context: OperationContext) -> List[str]:
        """
        Detect likely business domains from context clues.

        Args:
            context: Operation context to analyze

        Returns:
            List of detected business domain keywords
        """
        domains = []

        # Combine all text for analysis
        all_text = " ".join(
            [
                context.operation_name.lower(),
                context.pipeline_name.lower(),
                " ".join(context.source_connections).lower(),
                " ".join(context.destination_connections).lower(),
                context.transformation_summary.lower(),
            ]
        )

        # Domain keyword mapping
        domain_keywords = {
            "customer": ["customer", "client", "user", "account", "contact"],
            "sales": ["sales", "order", "revenue", "transaction", "purchase"],
            "finance": ["finance", "payment", "invoice", "billing", "cost"],
            "inventory": ["inventory", "product", "stock", "warehouse", "item"],
            "employee": ["employee", "staff", "hr", "person", "worker"],
            "geography": ["geography", "location", "address", "region", "territory"],
            "time": ["date", "time", "calendar", "period", "schedule"],
            "staging": ["staging", "temp", "intermediate", "buffer"],
            "warehouse": ["warehouse", "dwh", "dimension", "fact", "mart"],
            "operational": ["ods", "operational", "source", "transactional"],
        }

        # Check for domain keywords
        for domain, keywords in domain_keywords.items():
            if any(keyword in all_text for keyword in keywords):
                domains.append(domain)

        return domains

    def test_prompt_variations(self, context: OperationContext) -> Dict[str, str]:
        """
        Generate multiple prompt variations for testing and comparison.

        Args:
            context: Operation context to test with

        Returns:
            Dictionary of prompt variations for testing
        """
        variations = {
            "business_standard": self.create_business_prompt(context),
            "technical_detailed": self.create_technical_prompt(context),
            "context_relationship": self.create_context_prompt(context),
        }

        # Add domain-specific variation if domains detected
        detected_domains = self._detect_business_domains(context)
        if detected_domains:
            domain_hints = {"detected_domains": ", ".join(detected_domains)}
            variations["business_domain_aware"] = self.create_domain_specific_prompt(
                context, domain_hints
            )

        return variations

    def create_pipeline_business_prompt(
        self, pipeline_context: "PipelineContext"
    ) -> str:
        """
        Create a business-focused prompt for pipeline summary.

        Args:
            pipeline_context: Structured pipeline context

        Returns:
            Formatted prompt ready for LLM
        """
        template = """Based on the following SSIS pipeline context, provide a concise 1-2 sentence business summary that explains WHAT this pipeline accomplishes and WHY it's important for the business:

Pipeline: {pipeline_name}
Operations: {operation_count} operations
Data Sources: {sources}
Data Destinations: {destinations}
Data Flow: {data_flow_summary}

Focus on:
- Business purpose (what business need does this pipeline serve?)
- Data value (what insights or capabilities does this pipeline enable?)
- Overall workflow objective
- Avoid technical jargon - explain in business terms

Business Summary:"""

        return template.format(
            pipeline_name=pipeline_context.pipeline_name,
            operation_count=pipeline_context.operation_count,
            sources=self._format_connections(pipeline_context.source_tables),
            destinations=self._format_connections(pipeline_context.destination_tables),
            data_flow_summary=pipeline_context.data_flow_summary,
        )

    def create_pipeline_domain_specific_prompt(
        self, pipeline_context: "PipelineContext", domain_hints: Dict[str, str]
    ) -> str:
        """
        Create a domain-specific business prompt for pipeline with additional context clues.

        Args:
            pipeline_context: Structured pipeline context
            domain_hints: Additional domain-specific information

        Returns:
            Enhanced business prompt with domain context
        """
        base_prompt = self.create_pipeline_business_prompt(pipeline_context)

        # Add domain-specific context
        domain_context = self._build_pipeline_domain_context(
            pipeline_context, domain_hints
        )
        if domain_context:
            enhanced_prompt = (
                base_prompt
                + f"\n\nDomain Context: {domain_context}\n\nBusiness Summary:"
            )
            return enhanced_prompt

        return base_prompt

    def _detect_pipeline_domains(
        self, pipeline_context: "PipelineContext"
    ) -> List[str]:
        """
        Detect business domains from pipeline context for domain-specific prompting.

        Args:
            pipeline_context: Structured pipeline context

        Returns:
            List of detected domain keywords
        """
        domains = []

        # Check pipeline name
        pipeline_name_lower = pipeline_context.pipeline_name.lower()

        # Check tables
        all_tables = (
            pipeline_context.source_tables + pipeline_context.destination_tables
        )
        all_tables_str = " ".join(all_tables).lower()

        # Domain detection patterns
        domain_patterns = {
            "customer": ["customer", "client", "account", "contact"],
            "sales": ["sales", "order", "transaction", "purchase", "revenue"],
            "employee": ["employee", "staff", "hr", "person", "worker"],
            "product": ["product", "item", "inventory", "catalog"],
            "finance": ["finance", "accounting", "budget", "cost", "profit"],
            "warehouse": ["warehouse", "dwh", "fact", "dim", "etl"],
            "staging": ["staging", "stg", "temp", "ods"],
            "reporting": ["report", "dashboard", "analytics", "bi"],
        }

        for domain, keywords in domain_patterns.items():
            if any(
                keyword in pipeline_name_lower or keyword in all_tables_str
                for keyword in keywords
            ):
                domains.append(domain)

        return domains

    def _build_pipeline_domain_context(
        self, pipeline_context: "PipelineContext", domain_hints: Dict[str, str]
    ) -> str:
        """
        Build domain-specific context string for pipeline prompting.

        Args:
            pipeline_context: Structured pipeline context
            domain_hints: Domain-specific hints

        Returns:
            Formatted domain context string
        """
        detected_domains = domain_hints.get("detected_domains", "")
        if not detected_domains:
            detected_domains = ", ".join(
                self._detect_pipeline_domains(pipeline_context)
            )

        if not detected_domains:
            return ""

        domain_context_parts = [f"This appears to be a {detected_domains} pipeline"]

        # Add specific domain guidance
        if "customer" in detected_domains:
            domain_context_parts.append(
                "focusing on customer data management and relationships"
            )
        elif "sales" in detected_domains:
            domain_context_parts.append(
                "supporting sales operations and revenue tracking"
            )
        elif "warehouse" in detected_domains:
            domain_context_parts.append(
                "building analytical data warehouse capabilities"
            )
        elif "employee" in detected_domains:
            domain_context_parts.append(
                "managing employee information and HR processes"
            )

        return ". ".join(domain_context_parts) + "."

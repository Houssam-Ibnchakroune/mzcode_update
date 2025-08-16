"""
Traceability utilities for maintaining links between graph elements and source files.

This module provides standardized functions to create consistent traceability properties
for both nodes and edges in the knowledge graph, ensuring every element can be traced
back to its original source file and context.
"""

from typing import Dict, Any, Optional
from pathlib import Path


class SourceContext:
    """Standardized source context for nodes and edges"""
    
    @staticmethod
    def create_node_traceability(
        source_file_path: str,
        source_file_type: str = "dtsx",
        xml_path: Optional[str] = None,
        line_number: Optional[int] = None,
        parent_package: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create standardized traceability properties for nodes.
        
        Args:
            source_file_path: Full path to the source file
            source_file_type: Type of source file (dtsx, conmgr, params)
            xml_path: XPath to the element in the XML file
            line_number: Line number in the source file
            parent_package: Name of parent package for cross-references
            
        Returns:
            Dictionary with standardized traceability properties
        """
        context = {
            "source_file_path": str(Path(source_file_path).resolve()),
            "source_file_type": source_file_type,
            "technology": "SSIS"
        }
        
        if xml_path:
            context["xml_path"] = xml_path
        if line_number:
            context["line_number"] = line_number
        if parent_package:
            context["parent_package"] = parent_package
            
        return context

    @staticmethod
    def create_edge_traceability(
        source_file_path: str,
        derivation_method: str,
        xml_location: Optional[str] = None,
        context_info: Optional[Dict[str, Any]] = None,
        confidence_level: str = "high"
    ) -> Dict[str, Any]:
        """
        Create standardized traceability properties for edges.
        
        Args:
            source_file_path: Full path to the source file
            derivation_method: How the relationship was derived 
                             (xml_metadata|sql_parsing|data_flow_analysis|inference)
            xml_location: XPath to the XML element that defined this relationship
            context_info: Additional context about how the relationship was derived
            confidence_level: Confidence in the relationship (high|medium|low)
            
        Returns:
            Dictionary with standardized traceability properties
        """
        context = {
            "source_file_path": str(Path(source_file_path).resolve()),
            "derivation_method": derivation_method,
            "confidence_level": confidence_level,
            "technology": "SSIS"
        }
        
        if xml_location:
            context["xml_location"] = xml_location
        if context_info:
            context["context_info"] = context_info
            
        return context

    @staticmethod
    def create_sql_derivation_context(
        sql_statement: str,
        component_type: Optional[str] = None,
        property_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create context info for SQL-derived relationships.
        
        Args:
            sql_statement: The SQL statement that established the relationship
            component_type: Type of SSIS component (e.g., "Execute SQL Task")
            property_name: Name of the property containing the SQL (e.g., "SqlCommand")
            
        Returns:
            Dictionary with SQL derivation context
        """
        context = {
            "sql_statement": sql_statement[:500],  # Truncate for storage
            "sql_statement_length": len(sql_statement)
        }
        
        if component_type:
            context["component_type"] = component_type
        if property_name:
            context["property_name"] = property_name
            
        return context

    @staticmethod
    def create_dataflow_derivation_context(
        component_type: str,
        component_name: str,
        input_name: Optional[str] = None,
        output_name: Optional[str] = None,
        transformation_details: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Create context info for data flow-derived relationships.
        
        Args:
            component_type: Type of data flow component (e.g., "OLE DB Source")
            component_name: Name of the component instance
            input_name: Name of input if applicable
            output_name: Name of output if applicable
            transformation_details: Details about transformations applied
            
        Returns:
            Dictionary with data flow derivation context
        """
        context = {
            "component_type": component_type,
            "component_name": component_name
        }
        
        if input_name:
            context["input_name"] = input_name
        if output_name:
            context["output_name"] = output_name
        if transformation_details:
            context["transformation_details"] = transformation_details
            
        return context

    @staticmethod
    def create_xml_derivation_context(
        xml_element_name: str,
        xml_attribute: Optional[str] = None,
        xml_property: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create context info for XML metadata-derived relationships.
        
        Args:
            xml_element_name: Name of the XML element
            xml_attribute: Attribute that established the relationship
            xml_property: Property that established the relationship
            
        Returns:
            Dictionary with XML derivation context
        """
        context = {
            "xml_element_name": xml_element_name
        }
        
        if xml_attribute:
            context["xml_attribute"] = xml_attribute
        if xml_property:
            context["xml_property"] = xml_property
            
        return context


class TraceabilityValidator:
    """Utilities for validating traceability in graph structures"""
    
    @staticmethod
    def validate_node_traceability(node_dict: Dict[str, Any]) -> Dict[str, bool]:
        """
        Validate that a node has proper traceability information.
        
        Args:
            node_dict: Node dictionary representation
            
        Returns:
            Dictionary with validation results
        """
        properties = node_dict.get("properties", {})
        
        return {
            "has_source_file_path": "source_file_path" in properties,
            "has_source_file_type": "source_file_type" in properties,
            "has_technology": "technology" in properties,
            "is_valid_file_path": (
                "source_file_path" in properties and 
                bool(properties.get("source_file_path"))
            )
        }
    
    @staticmethod
    def validate_edge_traceability(edge_dict: Dict[str, Any]) -> Dict[str, bool]:
        """
        Validate that an edge has proper traceability information.
        
        Args:
            edge_dict: Edge dictionary representation
            
        Returns:
            Dictionary with validation results
        """
        properties = edge_dict.get("properties", {})
        
        return {
            "has_source_file_path": "source_file_path" in properties,
            "has_derivation_method": "derivation_method" in properties,
            "has_confidence_level": "confidence_level" in properties,
            "has_technology": "technology" in properties,
            "is_valid_derivation": (
                "derivation_method" in properties and 
                properties.get("derivation_method") in [
                    "xml_metadata", "sql_parsing", "data_flow_analysis", "inference"
                ]
            )
        }
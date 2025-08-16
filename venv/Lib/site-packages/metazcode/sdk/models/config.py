"""Configuration models for MetaZenseCode."""

from typing import Optional, Literal
from pydantic import BaseModel, Field
import os


class DatabaseConfig(BaseModel):
    """Database configuration for graph storage."""
    
    backend: Literal["networkx", "memgraph"] = Field(
        default="networkx",
        description="Graph database backend to use"
    )
    
    # Memgraph connection settings
    host: str = Field(
        default="localhost",
        description="Memgraph server host"
    )
    
    port: int = Field(
        default=7687,
        description="Memgraph server port"
    )
    
    username: Optional[str] = Field(
        default=None,
        description="Memgraph username"
    )
    
    password: Optional[str] = Field(
        default=None,
        description="Memgraph password"
    )
    
    database: str = Field(
        default="memgraph",
        description="Database name"
    )
    
    connection_timeout: int = Field(
        default=30,
        description="Connection timeout in seconds"
    )
    
    @classmethod
    def from_environment(cls) -> "DatabaseConfig":
        """Create configuration from environment variables."""
        return cls(
            backend=os.getenv("METAZCODE_DB_BACKEND", "networkx"),
            host=os.getenv("MEMGRAPH_HOST", "localhost"),
            port=int(os.getenv("MEMGRAPH_PORT", "7687")),
            username=os.getenv("MEMGRAPH_USERNAME"),
            password=os.getenv("MEMGRAPH_PASSWORD"),
            database=os.getenv("MEMGRAPH_DATABASE", "memgraph"),
            connection_timeout=int(os.getenv("MEMGRAPH_TIMEOUT", "30"))
        )


class MetaZenseConfig(BaseModel):
    """Main configuration for MetaZenseCode."""
    
    database: DatabaseConfig = Field(
        default_factory=DatabaseConfig,
        description="Database configuration"
    )
    
    # Analysis settings
    enable_cross_package_analysis: bool = Field(
        default=True,
        description="Enable cross-package dependency analysis"
    )
    
    enable_enhanced_indexing: bool = Field(
        default=True,
        description="Enable enhanced SSIS indexing"
    )
    
    # Logging settings
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = Field(
        default="INFO",
        description="Logging level"
    )
    
    @classmethod
    def from_environment(cls) -> "MetaZenseConfig":
        """Create configuration from environment variables."""
        return cls(
            database=DatabaseConfig.from_environment(),
            enable_cross_package_analysis=os.getenv("METAZCODE_ENABLE_CROSS_ANALYSIS", "true").lower() == "true",
            enable_enhanced_indexing=os.getenv("METAZCODE_ENABLE_INDEXING", "true").lower() == "true",
            log_level=os.getenv("METAZCODE_LOG_LEVEL", "INFO")
        )
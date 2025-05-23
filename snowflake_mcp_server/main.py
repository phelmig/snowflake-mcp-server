"""MCP server implementation for Snowflake.

This module provides a Model Context Protocol (MCP) server that allows Claude
to perform read-only operations against Snowflake databases. It connects to
Snowflake using either service account authentication with a private key or
external browser authentication. It exposes various tools for querying database
metadata and data, including support for multi-view and multi-database queries.

The server is designed to be used with Claude Desktop as an MCP server, providing
Claude with secure, controlled access to Snowflake data for analysis and reporting.
"""

import os
from typing import Any, Dict, List, Optional, Sequence, Union

import anyio
import mcp.types as mcp_types
import sqlglot
from dotenv import load_dotenv
from fastmcp import FastMCP, Context
from sqlglot.errors import ParseError

from snowflake_mcp_server.utils.snowflake_conn import (
    AuthType,
    SnowflakeConfig,
    connection_manager,
)

# Load environment variables from .env file
load_dotenv()


# Initialize Snowflake configuration from environment variables
def get_snowflake_config() -> SnowflakeConfig:
    """Load Snowflake configuration from environment variables."""
    auth_type_str = os.getenv("SNOWFLAKE_AUTH_TYPE", "private_key").lower()
    auth_type = (
        AuthType.PRIVATE_KEY
        if auth_type_str == "private_key"
        else AuthType.EXTERNAL_BROWSER
    )

    config = SnowflakeConfig(
        account=os.getenv("SNOWFLAKE_ACCOUNT", ""),
        user=os.getenv("SNOWFLAKE_USER", ""),
        auth_type=auth_type,
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema_name=os.getenv("SNOWFLAKE_SCHEMA"),
        role=os.getenv("SNOWFLAKE_ROLE"),
    )

    # Only set private_key_path if using private key authentication
    if auth_type == AuthType.PRIVATE_KEY:
        config.private_key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH", "")

    return config


# Initialize the connection manager at startup
def init_connection_manager() -> None:
    """Initialize the connection manager with Snowflake config."""
    config = get_snowflake_config()
    connection_manager.initialize(config)


# Create FastMCP server instance
mcp = FastMCP(
    name="snowflake-mcp-server",
    version="0.2.0",
    instructions="MCP server for performing read-only operations against Snowflake.",
    debug=True  # Enable debug mode for better error reporting
)


# Snowflake query handler functions
@mcp.tool()
async def list_databases() -> Sequence[Union[mcp_types.TextContent, mcp_types.ImageContent, mcp_types.EmbeddedResource]]:
    """List all accessible Snowflake databases."""
    try:
        # Get Snowflake connection from connection manager
        conn = connection_manager.get_connection()

        # Execute query
        cursor = conn.cursor()
        cursor.execute("SHOW DATABASES")

        # Process results
        databases = []
        for row in cursor:
            databases.append(row[1])  # Database name is in the second column

        cursor.close()
        # Don't close the connection, just the cursor

        # Return formatted content
        return [
            mcp_types.TextContent(
                type="text",
                text="Available Snowflake databases:\n" + "\n".join(databases),
            )
        ]

    except Exception as e:
        return [
            mcp_types.TextContent(
                type="text", text=f"Error querying databases: {str(e)}"
            )
        ]

@mcp.tool()
async def list_views(database: str, schema_name: Optional[str] = None) -> Sequence[Union[mcp_types.TextContent, mcp_types.ImageContent, mcp_types.EmbeddedResource]]:
    """List all views in a specified database and schema."""
    try:
        # Get Snowflake connection from connection manager
        conn = connection_manager.get_connection()

        if not database:
            return [
                mcp_types.TextContent(
                    type="text", text="Error: database parameter is required"
                )
            ]

        # Use the provided database and schema, or use default schema
        if database:
            conn.cursor().execute(f"USE DATABASE {database}")
        if schema_name:
            conn.cursor().execute(f"USE SCHEMA {schema_name}")
        else:
            # Get the current schema
            cursor = conn.cursor()
            cursor.execute("SELECT CURRENT_SCHEMA()")
            schema_result = cursor.fetchone()
            if schema_result:
                schema_name = schema_result[0]
            else:
                return [
                    mcp_types.TextContent(
                        type="text", text="Error: Could not determine current schema"
                    )
                ]

        # Execute query to list views
        cursor = conn.cursor()
        cursor.execute(f"SHOW VIEWS IN {database}.{schema_name}")

        # Process results
        views = []
        for row in cursor:
            view_name = row[1]  # View name is in the second column
            created_on = row[5]  # Creation date
            views.append(f"{view_name} (created: {created_on})")

        cursor.close()
        # Don't close the connection, just the cursor

        if views:
            return [
                mcp_types.TextContent(
                    type="text",
                    text=f"Views in {database}.{schema_name}:\n" + "\n".join(views),
                )
            ]
        else:
            return [
                mcp_types.TextContent(
                    type="text", text=f"No views found in {database}.{schema_name}"
                )
            ]

    except Exception as e:
        return [
            mcp_types.TextContent(type="text", text=f"Error listing views: {str(e)}")
        ]

@mcp.tool()
async def describe_view(database: str, view_name: str, schema_name: Optional[str] = None) -> Sequence[Union[mcp_types.TextContent, mcp_types.ImageContent, mcp_types.EmbeddedResource]]:
    """Get detailed information about a specific view including columns and SQL definition."""
    try:
        # Get Snowflake connection from connection manager
        conn = connection_manager.get_connection()

        if not database or not view_name:
            return [
                mcp_types.TextContent(
                    type="text",
                    text="Error: database and view_name parameters are required",
                )
            ]

        # Use the provided schema or use default schema
        if schema_name:
            full_view_name = f"{database}.{schema_name}.{view_name}"
        else:
            # Get the current schema
            cursor = conn.cursor()
            cursor.execute("SELECT CURRENT_SCHEMA()")
            schema_result = cursor.fetchone()
            if schema_result:
                schema_name = schema_result[0]
                full_view_name = f"{database}.{schema_name}.{view_name}"
            else:
                return [
                    mcp_types.TextContent(
                        type="text", text="Error: Could not determine current schema"
                    )
                ]

        # Execute query to describe view
        cursor = conn.cursor()
        cursor.execute(f"DESCRIBE VIEW {full_view_name}")

        # Process results
        columns = []
        for row in cursor:
            col_name = row[0]
            col_type = row[1]
            col_null = "NULL" if row[3] == "Y" else "NOT NULL"
            columns.append(f"{col_name} : {col_type} {col_null}")

        # Get view definition
        cursor.execute(f"SELECT GET_DDL('VIEW', '{full_view_name}')")
        view_ddl_result = cursor.fetchone()
        view_ddl = view_ddl_result[0] if view_ddl_result else "Definition not available"

        cursor.close()
        # Don't close the connection, just the cursor

        if columns:
            result = f"## View: {full_view_name}\n\n"
            result += "### Columns:\n"
            for col in columns:
                result += f"- {col}\n"

            result += "\n### View Definition:\n```sql\n"
            result += view_ddl
            result += "\n```"

            return [mcp_types.TextContent(type="text", text=result)]
        else:
            return [
                mcp_types.TextContent(
                    type="text",
                    text=f"View {full_view_name} not found or you don't have permission to access it.",
                )
            ]

    except Exception as e:
        return [
            mcp_types.TextContent(type="text", text=f"Error describing view: {str(e)}")
        ]

@mcp.tool()
async def query_view(database: str, view_name: str, schema_name: Optional[str] = None, limit: int = 10) -> Sequence[Union[mcp_types.TextContent, mcp_types.ImageContent, mcp_types.EmbeddedResource]]:
    """Query data from a view with an optional row limit."""
    try:
        # Get Snowflake connection from connection manager
        conn = connection_manager.get_connection()

        if not database or not view_name:
            return [
                mcp_types.TextContent(
                    type="text",
                    text="Error: database and view_name parameters are required",
                )
            ]

        # Use the provided schema or use default schema
        if schema_name:
            full_view_name = f"{database}.{schema_name}.{view_name}"
        else:
            # Get the current schema
            cursor = conn.cursor()
            cursor.execute("SELECT CURRENT_SCHEMA()")
            schema_result = cursor.fetchone()
            if schema_result:
                schema_name = schema_result[0]
                full_view_name = f"{database}.{schema_name}.{view_name}"
            else:
                return [
                    mcp_types.TextContent(
                        type="text", text="Error: Could not determine current schema"
                    )
                ]

        # Execute query to get data from view
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {full_view_name} LIMIT {limit}")

        # Get column names
        column_names = (
            [col[0] for col in cursor.description] if cursor.description else []
        )

        # Process results
        rows = cursor.fetchall()

        cursor.close()
        # Don't close the connection, just the cursor

        if rows:
            # Format the results as a markdown table
            result = f"## Data from {full_view_name} (Showing {len(rows)} rows)\n\n"

            # Create header row
            result += "| " + " | ".join(column_names) + " |\n"
            result += "| " + " | ".join(["---" for _ in column_names]) + " |\n"

            # Add data rows
            for row in rows:
                formatted_values = []
                for val in row:
                    if val is None:
                        formatted_values.append("NULL")
                    else:
                        # Format the value as string and escape any pipe characters
                        formatted_values.append(str(val).replace("|", "\\|"))
                result += "| " + " | ".join(formatted_values) + " |\n"

            return [mcp_types.TextContent(type="text", text=result)]
        else:
            return [
                mcp_types.TextContent(
                    type="text",
                    text=f"No data found in view {full_view_name} or the view is empty.",
                )
            ]

    except Exception as e:
        return [
            mcp_types.TextContent(type="text", text=f"Error querying view: {str(e)}")
        ]

@mcp.tool()
async def execute_query(query: str, database: Optional[str] = None, schema_name: Optional[str] = None, limit: int = 100) -> Sequence[Union[mcp_types.TextContent, mcp_types.ImageContent, mcp_types.EmbeddedResource]]:
    """Execute a read-only SQL query against Snowflake."""
    try:
        # Get Snowflake connection from connection manager
        conn = connection_manager.get_connection()

        if not query:
            return [
                mcp_types.TextContent(
                    type="text", text="Error: query parameter is required"
                )
            ]

        # Validate that the query is read-only
        try:
            parsed_statements = sqlglot.parse(query, dialect="snowflake")
            read_only_types = {"select", "show", "describe", "explain", "with"}

            if not parsed_statements:
                raise ParseError("Error: Could not parse SQL query")

            for stmt in parsed_statements:
                if (
                    stmt is not None
                    and hasattr(stmt, "key")
                    and stmt.key
                    and stmt.key.lower() not in read_only_types
                ):
                    raise ParseError(
                        f"Error: Only read-only queries are allowed. Found statement type: {stmt.key}"
                    )

        except ParseError as e:
            return [
                mcp_types.TextContent(
                    type="text",
                    text=f"Error: Only SELECT/SHOW/DESCRIBE/EXPLAIN/WITH queries are allowed for security reasons. {str(e)}",
                )
            ]

        # Use the specified database and schema if provided
        if database:
            conn.cursor().execute(f"USE DATABASE {database}")
        if schema_name:
            conn.cursor().execute(f"USE SCHEMA {schema_name}")

        # Extract database and schema context info for logging/display
        context_cursor = conn.cursor()
        context_cursor.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()")
        context_result = context_cursor.fetchone()
        if context_result:
            current_db, current_schema = context_result
        else:
            current_db, current_schema = "Unknown", "Unknown"
        context_cursor.close()

        # Ensure the query has a LIMIT clause to prevent large result sets
        # Parse the query to check if it already has a LIMIT
        if "LIMIT " not in query.upper():
            # Remove any trailing semicolon before adding the LIMIT clause
            query = query.rstrip().rstrip(";")
            query = f"{query} LIMIT {limit};"

        # Execute the query
        cursor = conn.cursor()
        cursor.execute(query)

        # Get column names and types
        column_names = (
            [col[0] for col in cursor.description] if cursor.description else []
        )

        # Fetch only up to limit_rows
        rows = cursor.fetchmany(limit)
        row_count = len(rows) if rows else 0

        cursor.close()
        # Don't close the connection, just the cursor

        if rows:
            # Format the results as a markdown table
            result = f"## Query Results (Database: {current_db}, Schema: {current_schema})\n\n"
            result += f"Showing {row_count} row{'s' if row_count != 1 else ''}\n\n"
            result += f"```sql\n{query}\n```\n\n"

            # Create header row
            result += "| " + " | ".join(column_names) + " |\n"
            result += "| " + " | ".join(["---" for _ in column_names]) + " |\n"

            # Add data rows
            for row in rows:
                formatted_values = []
                for val in row:
                    if val is None:
                        formatted_values.append("NULL")
                    else:
                        # Format the value as string and escape any pipe characters
                        # Truncate very long values to prevent huge tables
                        val_str = str(val).replace("|", "\\|")
                        if len(val_str) > 200:  # Truncate long values
                            val_str = val_str[:197] + "..."
                        formatted_values.append(val_str)
                result += "| " + " | ".join(formatted_values) + " |\n"

            return [mcp_types.TextContent(type="text", text=result)]
        else:
            return [
                mcp_types.TextContent(
                    type="text",
                    text=f"Query executed successfully in {current_db}.{current_schema}, but returned no results.",
                )
            ]

    except Exception as e:
        return [
            mcp_types.TextContent(type="text", text=f"Error executing query: {str(e)}")
        ]

# Function to run the server with stdio interface
def run_stdio_server() -> None:
    """Run the MCP server using stdin/stdout for communication."""
    # Initialize the connection manager before running the server
    init_connection_manager()

    mcp.run(transport="streamable-http", host="127.0.0.1", port=8000, path="/snoflake")

# MCP Server for Snowflake

A Model Context Protocol (MCP) server for performing read-only operations against Snowflake databases. This tool enables AI assistants to securely query Snowflake data without modifying any information.

## Features

- Flexible authentication to Snowflake using either:
  - Service account authentication with private key
  - External browser authentication for interactive sessions
- Connection pooling with automatic background refresh to maintain persistent connections
- Support for querying multiple views and databases in a single session
- Support for multiple SQL statement types (SELECT, SHOW, DESCRIBE, EXPLAIN, WITH)
- MCP-compatible handlers for querying Snowflake data
- Read-only operations with security checks to prevent data modification
- Support for Python 3.12+
- Streamable HTTP service for easy integration with any MCP client

## Available Tools

The server provides the following tools for querying Snowflake:

- **list_databases**: List all accessible Snowflake databases
- **list_views**: List all views in a specified database and schema
- **describe_view**: Get detailed information about a specific view including columns and SQL definition
- **query_view**: Query data from a view with an optional row limit
- **execute_query**: Execute custom read-only SQL queries (SELECT, SHOW, DESCRIBE, EXPLAIN, WITH) with results formatted as markdown tables

## Installation

### Prerequisites

- Python 3.12 or higher
- A Snowflake account with either:
  - A configured service account (username + private key), or
  - A regular user account for browser-based authentication
- [uv](https://github.com/astral-sh/uv) package manager (recommended)

### Steps

1. Clone this repository:
   ```
   git clone https://github.com/yourusername/snowflake-mcp-server.git
   cd snowflake-mcp-server
   ```

2. Install the package:
   ```
   uv pip install -e .
   ```

3. Create a `.env` file with your Snowflake credentials:

   Choose one of the provided example files based on your preferred authentication method:

   **For private key authentication**:
   ```
   cp .env.private_key.example .env
   ```
   Then edit the `.env` file to set your Snowflake account details and path to your private key.

   **For external browser authentication**:
   ```
   cp .env.browser.example .env
   ```
   Then edit the `.env` file to set your Snowflake account details.

## Usage

### Running the Server

After installing the package, you can run the server directly with:

```
uv run snowflake-mcp
```

This will start the MCP server as a streamable HTTP service on `http://127.0.0.1:8000/snowflake`. The server can be connected to any MCP client that supports HTTP communication.

When using external browser authentication, a browser window will automatically open prompting you to log in to your Snowflake account.

### MCP Client Integration

To connect to the server from an MCP client, use the following configuration:

```yaml
"snowflake-mcp-server": {
   "url": "http://127.0.0.1:8000/snowflake"
}
```

### Example Queries

When using with an AI assistant, you can ask questions like:

- "Can you list all the databases in my Snowflake account?"
- "List all views in the MARKETING database"
- "Describe the structure of the CUSTOMER_ANALYTICS view in the SALES database"
- "Show me sample data from the REVENUE_BY_REGION view in the FINANCE database"
- "Run this SQL query: SELECT customer_id, SUM(order_total) as total_spend FROM SALES.ORDERS GROUP BY customer_id ORDER BY total_spend DESC LIMIT 10"
- "Query the MARKETING database to find the top 5 performing campaigns by conversion rate"
- "Compare data from views in different databases by querying SALES.CUSTOMER_METRICS and MARKETING.CAMPAIGN_RESULTS"

### Configuration

The server can be configured through environment variables:

### Snowflake Connection Settings

- `SNOWFLAKE_AUTH_TYPE`: Authentication type (`private_key` or `external_browser`)
- `SNOWFLAKE_ACCOUNT`: Your Snowflake account identifier
- `SNOWFLAKE_USER`: Your Snowflake username
- `SNOWFLAKE_WAREHOUSE`: (Optional) Default warehouse to use
- `SNOWFLAKE_DATABASE`: (Optional) Default database to use
- `SNOWFLAKE_SCHEMA`: (Optional) Default schema to use
- `SNOWFLAKE_ROLE`: (Optional) Default role to use

### Private Key Authentication Settings

For private key authentication, you can provide the private key in two ways:

1. Using a private key file:
   ```
   SNOWFLAKE_PRIVATE_KEY_PATH=/path/to/your/private_key.p8
   ```

2. Embedding the private key directly (useful for cloud environments):
   ```
   SNOWFLAKE_PRIVATE_KEY=-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEF...\n-----END PRIVATE KEY-----
   ```

   Note: If both `SNOWFLAKE_PRIVATE_KEY` and `SNOWFLAKE_PRIVATE_KEY_PATH` are set, the embedded key takes precedence.

### Server Configuration

- `MCP_SERVER_PORT`: Port to run the HTTP server on (default: 8000)
- `MCP_SERVER_PATH`: URL path for the MCP server (default: /snowflake)

### Connection Pooling Settings

- `SNOWFLAKE_CONN_REFRESH_HOURS`: Time interval in hours between connection refreshes (default: 8)

Example `.env` configuration:
```
# Authentication
SNOWFLAKE_AUTH_TYPE=private_key
SNOWFLAKE_ACCOUNT=your_account_id.your_region
SNOWFLAKE_USER=your_username

# Private key (embedded)
SNOWFLAKE_PRIVATE_KEY=-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEF...\n-----END PRIVATE KEY-----

# Server settings
MCP_SERVER_PORT=8080
MCP_SERVER_PATH=/api/snowflake

# Connection pooling
SNOWFLAKE_CONN_REFRESH_HOURS=4
```

## Authentication Methods

### Private Key Authentication

This method uses a service account and private key for non-interactive authentication, ideal for automated processes.

1. Create a key pair for your Snowflake user following [Snowflake documentation](https://docs.snowflake.com/en/user-guide/key-pair-auth)
2. Set `SNOWFLAKE_AUTH_TYPE=private_key` in your `.env` file
3. Provide the path to your private key in `SNOWFLAKE_PRIVATE_KEY_PATH`

### External Browser Authentication

This method opens a browser window for interactive authentication.

1. Set `SNOWFLAKE_AUTH_TYPE=external_browser` in your `.env` file
2. When you start the server, a browser window will open asking you to log in
3. After authentication, the session will remain active for the duration specified by your Snowflake account settings

## Security Considerations

This server:
- Enforces read-only operations (only SELECT, SHOW, DESCRIBE, EXPLAIN, and WITH statements are allowed)
- Automatically adds LIMIT clauses to prevent large result sets
- Uses secure authentication methods for connections to Snowflake
- Validates inputs to prevent SQL injection

⚠️ **Important**: Keep your `.env` file secure and never commit it to version control. The `.gitignore` file is configured to exclude it.

## Development

### Static Type Checking

```
mypy mcp_server_snowflake/
```

### Linting

```
ruff check .
```

### Formatting

```
ruff format .
```

### Running Tests

```
pytest
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Technical Details

This project uses:
- [Snowflake Connector Python](https://docs.snowflake.com/en/developer-guide/python-connector/python-connector) for connecting to Snowflake
- [FastMCP](https://github.com/jlowin/fastmcp) for the MCP server implementation
- [Pydantic](https://docs.pydantic.dev/) for data validation
- [python-dotenv](https://github.com/theskumar/python-dotenv) for environment variable management

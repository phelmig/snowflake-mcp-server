#!/bin/bash

# Exit on any error
set -e

echo "Starting bootstrap process..."

# Check Python version
python_version=$(python3 --version 2>&1 | awk '{print $2}')
required_version="3.12.0"

if [ "$(printf '%s\n' "$required_version" "$python_version" | sort -V | head -n1)" != "$required_version" ]; then
    echo "Error: Python 3.12 or higher is required. Found version $python_version"
    exit 1
fi

# Install uv if not already installed
if ! command -v uv &> /dev/null; then
    echo "Installing uv package manager..."
    curl -LsSf https://astral.sh/uv/install.sh | sh
    export PATH="$HOME/.cargo/bin:$PATH"
fi

# Create and activate virtual environment
echo "Setting up virtual environment..."
python3 -m venv .venv
source .venv/bin/activate

# Install uv in the virtual environment
echo "Installing uv in virtual environment..."
pip install uv

# Install project dependencies
echo "Installing project dependencies..."
uv pip install -e .


# Run the application
echo "Starting the application..."
uv run snowflake-mcp 
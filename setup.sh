#!/bin/bash
# One-shot setup script for FHIR Lakehouse project
# Usage: ./setup.sh

set -e

echo "🏥  FHIR Lakehouse — Setup"
echo "=========================="

# Check Python version
PYTHON_VERSION=$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
echo "✓ Python version: $PYTHON_VERSION"

# Check Java
if ! command -v java &> /dev/null; then
    echo "✗ Java not found. Install with: brew install openjdk@17"
    exit 1
fi
echo "✓ Java: $(java -version 2>&1 | head -1)"

# Create virtual environment
if [ ! -d "venv" ]; then
    echo "→ Creating virtual environment..."
    python3 -m venv venv
fi

# Activate and install
source venv/bin/activate
echo "→ Upgrading pip..."
pip install --upgrade pip --quiet

echo "→ Installing dependencies (this takes a minute)..."
pip install -r requirements.txt --quiet

# Create data directory placeholders
mkdir -p data/bronze data/silver data/gold

echo ""
echo "✅ Setup complete."
echo ""
echo "Next steps:"
echo "  1. source venv/bin/activate"
echo "  2. python run_pipeline.py"
echo ""

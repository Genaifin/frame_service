#!/bin/bash

echo "🎯 ValidusBoxes Quick Start (Non-Docker) - Port 80"
echo "==================================================="

# Check for root privileges (required for port 80)
if [ "$EUID" -ne 0 ]; then
    echo -e "${RED}❌ Root privileges required to run on port 80${NC}"
    echo -e "${YELLOW}💡 Solutions:${NC}"
    echo "   1. Run with sudo: sudo ./start_server.sh"
    echo "   2. Change PORT=80 to PORT=8000 in .env file"
    echo "   3. Use nginx as reverse proxy on port 80"
    exit 1
fi

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo -e "${YELLOW}⚠️  Virtual environment not found. Creating one...${NC}"
    python3 -m venv venv
    if [ $? -ne 0 ]; then
        echo -e "${RED}❌ Failed to create virtual environment${NC}"
        exit 1
    fi
    
    echo -e "${BLUE}📦 Installing dependencies...${NC}"
    source venv/bin/activate
    pip install -r requirements.txt
    if [ $? -ne 0 ]; then
        echo -e "${RED}❌ Failed to install dependencies${NC}"
        exit 1
    fi
else
    echo -e "${GREEN}✅ Virtual environment found${NC}"
    source venv/bin/activate
fi

# Create required directories
echo -e "${BLUE}📁 Creating required directories...${NC}"
mkdir -p temp
mkdir -p output_documents
mkdir -p data/frameDemo/l0
mkdir -p data/frameDemo/l1
mkdir -p queue

# Check if .env exists
if [ ! -f ".env" ]; then
    echo -e "${BLUE}🔧 Creating .env file...${NC}"
    cat > .env << EOF
# Server Configuration for Non-Docker Deployment
HOST=0.0.0.0
PORT=8000
SERVER_DOMAIN=dev.aithonsolutions.com
SERVER_IP=34.237.142.231

# Database Configuration for Local PostgreSQL
DB_TYPE=postgresql
DB_HOST=localhost
DB_PORT=5432
DB_NAME=validus_boxes
DB_USER=postgres
DB_PASSWORD=

# Application Configuration
BASE_PATH=.
TEMP_DIR=./temp
OUTPUT_DIR=./output_documents
BACKEND_OUTPUT_DIR=./data/frameDemo/l1
ENABLE_BACKEND_OUTPUT=true

# CORS Configuration
ALLOWED_ORIGINS=*
CORS_CREDENTIALS=true

# Logging Configuration
LOG_LEVEL=INFO
EOF
    echo -e "${GREEN}✅ Created .env file${NC}"
    echo -e "${YELLOW}⚠️  Please update database credentials in .env file if needed${NC}"
fi

# Check if PostgreSQL is running
if command -v pg_isready &> /dev/null; then
    if pg_isready -q; then
        echo -e "${GREEN}✅ PostgreSQL is running${NC}"
    else
        echo -e "${YELLOW}⚠️  PostgreSQL may not be running${NC}"
        echo "Please ensure PostgreSQL is installed and running"
    fi
fi

# Run database migrations (optional)
if command -v alembic &> /dev/null; then
    echo -e "${BLUE}🔄 Running database migrations...${NC}"
    alembic upgrade head
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✅ Database migrations completed${NC}"
    else
        echo -e "${YELLOW}⚠️  Migration issues detected. Server may not work properly.${NC}"
    fi
else
    echo -e "${YELLOW}⚠️  Alembic not found. Installing...${NC}"
    pip install alembic
    alembic upgrade head
fi

echo -e "${BLUE}🚀 Starting ValidusBoxes server...${NC}"
echo -e "${GREEN}📍 Server will be accessible at:${NC}"
echo "   - Local: http://localhost:8000"
echo "   - External: http://dev.aithonsolutions.com"
echo "   - API Docs: http://dev.aithonsolutions.com/docs"
echo ""
echo "Press Ctrl+C to stop the server"
echo "--------------------------------------------------"

# Start the server
python -m uvicorn server.APIServer:app --host 0.0.0.0 --port 8000 --reload --log-config uvicornLogConfig.yaml

echo ""
echo -e "${RED}🛑 Server stopped${NC}"

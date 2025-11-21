## Local Development Setup

python 3.11.9

1. Create a virtual environment:

```bash
python -m venv venv
```

2. Activate the virtual environment:

- Windows:

```bash
.\venv\Scripts\activate
```

- Unix/MacOS:

```bash
source venv/bin/activate
```

3. Install dependencies:

```bash
pip install -r requirements.txt
```

## Running the Server Locally

Start the server with:

```bash
uvicorn server.APIServer:app --reload
```

```bash
python3 -m uvicorn server.APIServer:app --reload --log-config=uvicornLogConfig.yaml
```

The server will start at `http://localhost`, accessible via `http://dev.aithonsolutions.com` or `http://34.237.142.231`

## Running the Server for External Access

To run the server accessible via domain name or IP address (dev.aithonsolutions.com / 34.237.142.231):

```bash
# Note: Port 80 requires administrator/root privileges
sudo uvicorn server.APIServer:app --host 0.0.0.0 --port 80 --reload
```

```bash
sudo python3 -m uvicorn server.APIServer:app --host 0.0.0.0 --port 80 --reload --log-config=uvicornLogConfig.yaml
```

This will make the server accessible from external networks via `http://dev.aithonsolutions.com` or `http://34.237.142.231`

⚠️ **Important**: Running on port 80 requires administrator/root privileges

## Docker Deployment

1. Build the Docker image:

```bash
docker docker build -t backend:dev .
```

2. Run the container:

```bash
docker run -d -p 8000:8000 backend:dev
```

For external access via IP address:

```bash
docker run -d -p 8000:8000 -e HOST=0.0.0.0 -e PORT=8000 backend:dev
```

The server will be available at `http://localhost:8000`, `http://dev.aithonsolutions.com:8000` or `http://34.237.142.231:8000` for external access

## API Documentation

Once the server is running, you can access:

- Swagger UI: `http://localhost:8000/docs`, `http://dev.aithonsolutions.com:8000/docs` or `http://34.237.142.231/docs`
- ReDoc: `http://localhost:8000/redoc`, `http://dev.aithonsolutions.com:8000/redoc` or `http://34.237.142.231/redoc`

## Troubleshooting 502 Bad Gateway

If you encounter a 502 Bad Gateway error when accessing the API via IP address, follow these steps:

### 1. Start the Backend Server

Make sure the backend server is running on port 8082:

```bash
# Using Docker (recommended for production)
./deploy.sh

# Or run manually
uvicorn server.APIServer:app --host 0.0.0.0 --port 8000 --reload
```

### 2. Check Server Status

Verify the backend is running:

```bash
# Check if port 8082 is in use (Docker deployment)
netstat -ano | findstr :8082

# Check if port 8000 is in use (manual deployment)
netstat -ano | findstr :8000
```

### 3. Test Direct Backend Access

Test the backend directly:

```bash
curl http://localhost:8082/api/v1/login
# or
curl http://localhost:8000/api/v1/login
```

### 4. Reload Nginx Configuration

After making changes to nginx config:

```bash
sudo nginx -t  # Test configuration
sudo systemctl reload nginx  # Reload if test passes
```

### 5. Check Nginx Error Logs

```bash
sudo tail -f /var/log/nginx/error.log
```

## Running Without Docker at IP 34.237.142.231

For production deployment without Docker containers, follow these steps:

### Prerequisites

1. **Python 3.11.9**: Ensure Python 3.11.9 is installed
2. **PostgreSQL**: Install and configure PostgreSQL database
3. **System Dependencies**: Install required system packages

### Step 1: Install System Dependencies

#### Ubuntu/Debian:
```bash
sudo apt-get update
sudo apt-get install -y tesseract-ocr tesseract-ocr-eng poppler-utils libglib2.0-0
```

#### Windows:
- Install Tesseract OCR from: https://github.com/UB-Mannheim/tesseract/wiki
- Install Poppler: Download from https://blog.alivate.com.au/poppler-windows/

### Step 2: Set Up Virtual Environment

```bash
# Create virtual environment
python3.11 -m venv venv

# Activate virtual environment
# Windows:
.\venv\Scripts\activate
# Linux/Mac:
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### Step 3: Configure PostgreSQL Database

1. **Install PostgreSQL** (if not already installed):
```bash
# Ubuntu/Debian
sudo apt install postgresql postgresql-contrib

# Windows - Download from https://www.postgresql.org/download/windows/
```

2. **Create Database and User**:
```sql
-- Connect to PostgreSQL as superuser
sudo -u postgres psql

-- Create database
CREATE DATABASE validus_boxes;

-- Create user without password (for trust authentication)
CREATE USER validus_user;
GRANT ALL PRIVILEGES ON DATABASE validus_boxes TO validus_user;

-- Exit
\q
```

3. **Configure PostgreSQL for passwordless authentication**:
```bash
# Edit PostgreSQL configuration for trust authentication
sudo nano /etc/postgresql/*/main/pg_hba.conf

# Change the local connection line to:
local   all             all                                     trust

# Restart PostgreSQL
sudo systemctl restart postgresql
```

4. **Update .env file** with your database credentials (no password):
```bash
DB_HOST=localhost
DB_PORT=5432
DB_NAME=validus_boxes
DB_USER=postgres
DB_PASSWORD=
```

### Step 4: Run Database Migrations

```bash
# Run Alembic migrations to set up database schema
alembic upgrade head
```

### Step 5: Start the Server

```bash
# Start the server bound to all interfaces for external access (requires admin/root)
sudo uvicorn server.APIServer:app --host 0.0.0.0 --port 80 --reload

# Or with logging configuration
sudo python3 -m uvicorn server.APIServer:app --host 0.0.0.0 --port 80 --reload --log-config=uvicornLogConfig.yaml
```

### Step 6: Configure Firewall (if needed)

Ensure port 80 is open for external access:

#### Ubuntu/Linux:
```bash
sudo ufw allow 80
```

#### Windows:
```powershell
# Run as Administrator
New-NetFirewallRule -DisplayName "ValidusBoxes API" -Direction Inbound -Port 80 -Protocol TCP -Action Allow
```

Note: Port 80 is typically open by default on most systems for HTTP traffic.

### Step 7: Access the Application

The application will be accessible at:
- **Local**: `http://localhost`
- **External**: `http://34.237.142.231`
- **API Docs**: `http://34.237.142.231/docs`

⚠️ **Administrator Privileges Required**: Port 80 requires running as administrator (Windows) or root (Linux/Mac)

### Environment Variables for Non-Docker Deployment

Your `.env` file should contain:
```bash
HOST=0.0.0.0
PORT=80
SERVER_IP=34.237.142.231

DB_TYPE=postgresql
DB_HOST=localhost
DB_PORT=5432
DB_NAME=validus_boxes
DB_USER=postgres
DB_PASSWORD=

BASE_PATH=.
TEMP_DIR=./temp
OUTPUT_DIR=./output_documents
BACKEND_OUTPUT_DIR=./data/frameDemo/l1
ENABLE_BACKEND_OUTPUT=true
```

### Production Considerations

1. **Process Manager**: Use PM2, systemd, or supervisor to manage the process
2. **Reverse Proxy**: Configure nginx to proxy requests to the Python server
3. **SSL/TLS**: Set up SSL certificates for HTTPS
4. **Security**: Configure firewall rules and security groups properly

#### Using PM2 (Process Manager):
```bash
# Install PM2
npm install -g pm2

# Create ecosystem file
cat > ecosystem.config.js << EOF
module.exports = {
  apps: [{
    name: 'validus-boxes',
    script: 'uvicorn',
    args: 'server.APIServer:app --host 0.0.0.0 --port 80',
    interpreter: './venv/bin/python',
    instances: 1,
    autorestart: true,
    watch: false,
    max_memory_restart: '1G',
    env: {
      NODE_ENV: 'production'
    }
  }]
};
EOF

# Start with PM2
pm2 start ecosystem.config.js
pm2 save
pm2 startup
```

#### Creating a Systemd Service:
```bash
# Create service file
sudo tee /etc/systemd/system/validus-boxes.service > /dev/null << EOF
[Unit]
Description=Validus Boxes API
After=network.target

[Service]
Type=simple
User=your_username
WorkingDirectory=/path/to/validusBoxes
Environment=PATH=/path/to/validusBoxes/venv/bin
ExecStart=/path/to/validusBoxes/venv/bin/uvicorn server.APIServer:app --host 0.0.0.0 --port 80
Restart=always

[Install]
WantedBy=multi-user.target
EOF

# Enable and start service
sudo systemctl enable validus-boxes
sudo systemctl start validus-boxes
sudo systemctl status validus-boxes
```

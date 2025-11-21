FROM python:3.13-slim

# Install build dependencies temporarily for PyMuPDF compilation
# These will be removed after installation to keep image size minimal
RUN apt-get update && apt-get install -y \
    tesseract-ocr \
    tesseract-ocr-eng \
    poppler-utils \
    libpq-dev \
    postgresql-client \
    gcc \
    g++ \
    make \
    python3-dev \
    pkg-config \
    libtesseract-dev \
    libleptonica-dev \
    && rm -rf /var/lib/apt/lists/*

# Create directories for host library mounts (for runtime)
RUN mkdir -p /usr/lib/x86_64-linux-gnu \
    /usr/share/tesseract-ocr \
    /usr/bin \
    /usr/lib/tesseract-ocr

# Create ubuntu user
RUN useradd -m -s /bin/bash ubuntu

# Set working directory
WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .

# Upgrade pip and install wheel for better package building
RUN pip install --no-cache-dir --upgrade pip setuptools wheel

# Configure pip for faster installs and enable build cache
RUN pip config set global.no-cache-dir true && \
    pip config set global.disable-pip-version-check true && \
    pip config set global.timeout 60 && \
    pip config set global.cache-dir /tmp/pip-cache && \
    pip config set global.build-dir /tmp/pip-build

# Install psycopg2-binary first to avoid conflicts
RUN pip install --no-cache-dir --no-deps --root-user-action=ignore psycopg2-binary==2.9.10

# Install dependencies in optimized order (heavy packages first, then lighter ones)
RUN pip install --no-cache-dir --no-deps --root-user-action=ignore \
    numpy==2.1.1 \
    pandas==2.2.3 \
    PyMuPDF==1.24.10 \
    Pillow==10.4.0

# Install remaining dependencies
RUN pip install --no-cache-dir --root-user-action=ignore -r requirements.txt

# Verify psycopg2-binary installation
RUN python -c "import psycopg2; print('psycopg2-binary installed successfully')"

# Clean up build dependencies to reduce image size
# Keep only runtime libraries that are needed
RUN apt-get remove -y \
    gcc \
    g++ \
    make \
    python3-dev \
    pkg-config \
    libtesseract-dev \
    libleptonica-dev \
    && apt-get autoremove -y \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy application code - only necessary files and directories
COPY server/ ./server/
COPY utils/ ./utils/
COPY frontendUtils/ ./frontendUtils/
COPY rbac/ ./rbac/
COPY boxes/ ./boxes/
COPY clients/ ./clients/
COPY data/ ./data/
COPY queue/ ./queue/
COPY frameEngine/ ./frameEngine/
COPY athena/ ./athena/
COPY alembic/ ./alembic/
COPY scripts/ ./scripts/
COPY database_models.py .
COPY database_seeder.py .
COPY alembic.ini .
COPY runner_frame.py .
COPY storage.py .
COPY ingestor.py .
COPY processor.py .
COPY file.py .
COPY validations.py .
COPY uvicornLogConfig.yaml .
COPY start_server.sh .
COPY start_server.py .
COPY setup_nginx.sh .
COPY schema/ .

# Create temp directory inside the image
RUN mkdir -p /app/temp

# Set environment variable so your code uses this path
ENV TEMP_DIR=/app/temp

# Create output directories for proper file handling
RUN mkdir -p /app/output_documents /app/temp

# Set environment variables for Docker container
ENV OUTPUT_DIR=/app/output_documents
ENV BACKEND_OUTPUT_DIR=/app/data/frameDemo/l1
ENV ENABLE_BACKEND_OUTPUT=true

# Change ownership of app directory to ubuntu user
RUN chown -R ubuntu:ubuntu /app

# Switch to ubuntu user
USER ubuntu

EXPOSE 8000

# This container includes runtime libraries for tesseract-ocr, poppler-utils, and PostgreSQL
# Build dependencies are removed after compilation to keep image size minimal
# Optional: You can still mount host libraries to override container libraries if needed:
# docker run -v /usr/lib/x86_64-linux-gnu:/usr/lib/x86_64-linux-gnu:ro \
#            -v /usr/share/tesseract-ocr:/usr/share/tesseract-ocr:ro \
#            -v /usr/bin/tesseract:/usr/bin/tesseract:ro \
#            -v /usr/bin/pdftoppm:/usr/bin/pdftoppm:ro \
#            -v /usr/bin/pdfinfo:/usr/bin/pdfinfo:ro \
#            -v /usr/bin/pdftotext:/usr/bin/pdftotext:ro \
#            your-image-name

CMD ["uvicorn", "server.APIServer:app", "--host", "0.0.0.0", "--port", "8000"]
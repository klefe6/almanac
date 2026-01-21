# Multi-stage Docker build for Almanac Futures
FROM python:3.11-slim as base

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    unixodbc \
    unixodbc-dev \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Install Microsoft ODBC Driver for SQL Server
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql17 \
    && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create cache directory
RUN mkdir -p .cache

# Create non-root user
RUN groupadd -r almanac && useradd -r -g almanac almanac \
    && chown -R almanac:almanac /app
USER almanac

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8085/health || exit 1

# Expose port
EXPOSE 8085

# Default command
CMD ["python", "runalmanac.py", "--host", "0.0.0.0", "--port", "8085", "--no-debug"]

# Development stage
FROM base as development

# Switch back to root for development tools
USER root

# Install development dependencies
RUN pip install --no-cache-dir \
    pytest \
    pytest-cov \
    pytest-benchmark \
    black \
    flake8 \
    mypy

# Switch back to app user
USER almanac

# Override command for development
CMD ["python", "runalmanac.py", "--host", "0.0.0.0", "--port", "8085", "--debug"]

# Production stage
FROM base as production

# Production optimizations
ENV ALMANAC_ENV=production \
    CACHE_TYPE=redis \
    REDIS_HOST=redis \
    REDIS_PORT=6379 \
    LOG_LEVEL=INFO \
    LOG_FORMAT=json

# Install production-only dependencies
RUN pip install --no-cache-dir \
    gunicorn

# Production command
CMD ["gunicorn", "--bind", "0.0.0.0:8085", "--workers", "4", "--timeout", "120", "almanac.app:app.server"]

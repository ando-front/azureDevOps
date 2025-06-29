FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    unixodbc \
    unixodbc-dev \
    freetds-dev \
    freetds-bin \
    tdsodbc \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Install Microsoft ODBC Driver 18 for SQL Server
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
    && rm -rf /var/lib/apt/lists/*

# Configure ODBC to trust the server certificate
RUN echo "[ODBC Driver 18 for SQL Server]\nTrustServerCertificate=Yes" >> /etc/odbcinst.ini

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Set environment variables
ENV PYTHONPATH=/app
ENV PYTEST_CURRENT_TEST=""

# Create necessary directories
RUN mkdir -p /app/test_results /app/config

# Copy application code
COPY . .

# Default command - run E2E tests
CMD ["python", "-m", "pytest", "tests/e2e/", "-v", "--tb=short", "-m", "e2e", "--junitxml=/app/test_results/e2e_results.xml"]

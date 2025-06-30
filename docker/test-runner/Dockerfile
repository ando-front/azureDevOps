FROM python:3.11-slim

# Set OpenSSL security level to 1 to allow connections to SQL Server with self-signed certs
RUN sed -i 's/DEFAULT@SECLEVEL=2/DEFAULT@SECLEVEL=1/g' /etc/ssl/openssl.cnf

WORKDIR /app

# Install system dependencies
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    curl \
    unixodbc \
    unixodbc-dev \
    freetds-dev \
    tdsodbc \
    gcc \
    g++ \
    freetds-bin \
    && rm -rf /var/lib/apt/lists/*

# Install Microsoft ODBC Driver 18 for SQL Server
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
    && rm -rf /var/lib/apt/lists/*

# Configure ODBC driver with proper path and trust server certificate
RUN printf "[ODBC Driver 18 for SQL Server]\nDescription=Microsoft ODBC Driver 18 for SQL Server\nDriver=/opt/microsoft/msodbcsql18/lib64/libmsodbcsql-18.so.1.1\nUsageCount=1\nTrustServerCertificate=Yes\n" >> /etc/odbcinst.ini

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
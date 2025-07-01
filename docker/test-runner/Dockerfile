FROM python:3.11-slim

# Set OpenSSL security level to 1 to allow connections to SQL Server with self-signed certs
RUN sed -i 's/DEFAULT@SECLEVEL=2/DEFAULT@SECLEVEL=1/g' /etc/ssl/openssl.cnf

WORKDIR /app

# Install system dependencies
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    curl \
    gnupg \
    lsb-release \
    unixodbc \
    unixodbc-dev \
    freetds-dev \
    tdsodbc \
    gcc \
    g++ \
    freetds-bin \
    && rm -rf /var/lib/apt/lists/*

# Install Microsoft ODBC Driver 18 for SQL Server (using modern GPG key management)
RUN curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && echo "deb [arch=amd64,arm64,armhf signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/repos/microsoft-debian-bullseye-prod bullseye main" > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 mssql-tools18 \
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

# Copy and setup entrypoint script
COPY docker/test-runner/run_e2e_tests_in_container.sh /usr/local/bin/run_e2e_tests_in_container.sh
RUN chmod +x /usr/local/bin/run_e2e_tests_in_container.sh

# Default command - run E2E tests
CMD ["python", "-m", "pytest", "tests/e2e/", "-v", "--tb=short", "-m", "e2e", "--junitxml=/app/test_results/e2e_results.xml"]
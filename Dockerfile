FROM node:18-bullseye-slim

# Build arguments for proxy configuration
ARG http_proxy
ARG https_proxy
ARG no_proxy

# Environment variables for proxy
ENV http_proxy=${http_proxy}
ENV https_proxy=${https_proxy}
ENV no_proxy=${no_proxy},127.0.0.1,localhost,devstoreaccount1
ENV DEBIAN_FRONTEND=noninteractive

# プロキシ設定の確認
RUN echo "Proxy settings: http_proxy=${http_proxy}, https_proxy=${https_proxy}, no_proxy=${no_proxy}"

# APT proxy configuration for better reliability
RUN if [ -n "$http_proxy" ]; then \
    echo "Acquire::http::Proxy \"$http_proxy\";" > /etc/apt/apt.conf.d/01proxy && \
    echo "Acquire::https::Proxy \"$https_proxy\";" >> /etc/apt/apt.conf.d/01proxy && \
    echo "Acquire::Retries \"3\";" >> /etc/apt/apt.conf.d/01proxy && \
    echo "Acquire::http::Timeout \"60\";" >> /etc/apt/apt.conf.d/01proxy && \
    echo "Acquire::https::Timeout \"60\";" >> /etc/apt/apt.conf.d/01proxy; \
    fi

# Package installation with improved error handling
RUN apt-get update --fix-missing && \
    apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    python3-venv \
    build-essential \
    curl \
    ca-certificates \
    openssh-server \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Python virtual environment setup
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
ENV PYTHONPATH="/tests"

# SSL certificate update
RUN update-ca-certificates

# pip configuration for proxy and trusted hosts
RUN mkdir -p /root/.pip
RUN if [ -n "$http_proxy" ]; then \
    echo "[global]" > /root/.pip/pip.conf && \
    echo "proxy = ${http_proxy}" >> /root/.pip/pip.conf && \
    echo "trusted-host = pypi.org pypi.python.org files.pythonhosted.org" >> /root/.pip/pip.conf && \
    echo "cert = /etc/ssl/certs/ca-certificates.crt" >> /root/.pip/pip.conf && \
    echo "timeout = 60" >> /root/.pip/pip.conf; \
    else \
    echo "[global]" > /root/.pip/pip.conf && \
    echo "trusted-host = pypi.org pypi.python.org files.pythonhosted.org" >> /root/.pip/pip.conf; \
    fi

# Upgrade pip first
RUN /opt/venv/bin/python -m pip install --upgrade pip \
    --trusted-host pypi.org \
    --trusted-host pypi.python.org \
    --trusted-host files.pythonhosted.org

# Install Python packages with improved error handling
RUN /opt/venv/bin/python -m pip install \
    azure-storage-blob==12.14.1 \
    pytest==7.3.1 \
    pytest-cov==4.0.0 \
    --trusted-host pypi.org \
    --trusted-host pypi.python.org \
    --trusted-host files.pythonhosted.org \
    --timeout 60 \
    --retries 3

# Verify pytest installation
RUN /opt/venv/bin/python -c "import pytest; print(f'pytest version: {pytest.__version__}')"

# npm configuration for proxy
RUN if [ -n "$http_proxy" ]; then \
    npm config set strict-ssl false && \
    npm config set registry "https://registry.npmjs.org/" && \
    npm config set proxy "$http_proxy" && \
    npm config set https-proxy "$https_proxy"; \
    fi

# Install Node.js packages (Azurite) - make optional for environments where it fails
RUN npm install -g @azure/storage-blob azurite || echo "Warning: Failed to install npm packages, continuing..."

# SFTP server configuration
RUN mkdir -p /var/run/sshd && \
    echo 'PermitRootLogin yes' >> /etc/ssh/sshd_config && \
    sed -i 's/#Port 22/Port 2222/g' /etc/ssh/sshd_config && \
    echo 'root:password' | chpasswd

# Copy test files and source code
COPY tests /tests
COPY src /tests/src

# Create necessary directories with proper permissions
RUN mkdir -p /tests/input /tests/output /tests/data/input /tests/data/output /tests/data/sftp /data && \
    chmod -R 777 /tests /data

# Expose required ports
EXPOSE 10000 10001 10002 2222 80

# Working directory
WORKDIR /tests

# Copy startup script
COPY startup.sh /startup.sh
RUN chmod +x /startup.sh

# Default command
CMD ["/startup.sh"]

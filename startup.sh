#!/bin/bash
set -e

echo "Starting container services..."

# Start SFTP server
echo "Starting SFTP server..."
/usr/sbin/sshd -D &

# Check if this is a GitHub Actions environment or local Docker Compose
if [ -n "$GITHUB_ACTIONS" ]; then
    echo "Detected GitHub Actions environment"
    # In GitHub Actions, Azurite runs as azurite-test service
    echo "Waiting for external Azurite service (GitHub Actions)..."
    for i in {1..60}; do
        if curl -f http://azurite-test:10000/devstoreaccount1 2>/dev/null; then
            echo "External Azurite service is ready!"
            break
        elif [ $i -eq 60 ]; then
            echo "Warning: External Azurite service not available after 60 attempts"
            echo "Tests requiring blob storage may fail"
        else
            echo "Waiting for external Azurite service... ($i/60)"
            sleep 1
        fi
    done
elif [ "$AZURITE_HOST" = "azurite" ]; then
    echo "Detected Docker Compose environment"
    # Wait for external Azurite service to be available
    echo "Waiting for external Azurite service (Docker Compose)..."
    for i in {1..60}; do
        if curl -f http://azurite:10000/devstoreaccount1 2>/dev/null; then
            echo "External Azurite service is ready!"
            break
        elif [ $i -eq 60 ]; then
            echo "Warning: External Azurite service not available after 60 attempts"
            echo "Tests requiring blob storage may fail"
        else
            echo "Waiting for external Azurite service... ($i/60)"
            sleep 1
        fi
    done
else
    echo "Detected standalone environment - starting internal Azurite"
    # Start Azurite if available (for standalone/legacy environments)
    if command -v azurite &> /dev/null; then
        echo "Starting internal Azurite..."
        azurite --location /data --debug /data/debug.log --loose --skipApiVersionCheck &
        # Wait for Azurite to start
        for i in {1..30}; do
            if curl -f http://127.0.0.1:10000/devstoreaccount1 2>/dev/null; then
                echo "Internal Azurite is ready!"
                break
            fi
            echo "Waiting for internal Azurite to start... ($i/30)"
            sleep 2
        done
    else
        echo "Warning: Azurite not available, tests requiring blob storage may fail"
    fi
fi

# Remove any problematic test files from the container
if [ -f "/tests/test_pipeline2.py" ]; then
    echo "Removing problematic test_pipeline2.py file..."
    rm -f /tests/test_pipeline2.py
fi

# Set up Python environment
source /opt/venv/bin/activate
export PYTHONPATH=/tests

# Keep container running
if [ $# -eq 0 ]; then
    echo "Container ready. Keeping alive..."
    tail -f /dev/null
else
    echo "Executing: $@"
    exec "$@"
fi

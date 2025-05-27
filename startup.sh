#!/bin/bash
set -e

echo "Starting container services..."

# Start SFTP server
echo "Starting SFTP server..."
/usr/sbin/sshd -D &

# Start Azurite if available
if command -v azurite &> /dev/null; then
    echo "Starting Azurite..."
    azurite --location /data --debug /data/debug.log --loose --skipApiVersionCheck &
    # Wait for Azurite to start
    for i in {1..30}; do
        if curl -f http://127.0.0.1:10000/devstoreaccount1 2>/dev/null; then
            echo "Azurite is ready!"
            break
        fi
        echo "Waiting for Azurite to start... ($i/30)"
        sleep 2
    done
else
    echo "Warning: Azurite not available, tests requiring blob storage may fail"
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

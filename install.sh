#!/bin/bash
set -e

echo "Installing Python dependencies..."
pip install --no-cache-dir kubernetes

# Install kubectl
python3 - <<'EOF'
import urllib.request, os
print('Downloading kubectl...')
url = 'https://storage.googleapis.com/kubernetes-release/release/v1.28.0/bin/linux/amd64/kubectl'
urllib.request.urlretrieve(url, '/tmp/kubectl')
os.chmod('/tmp/kubectl', 0o755)
print('kubectl ready')
EOF

mv /tmp/kubectl /usr/local/bin/kubectl

echo "Starting autoscaler..."
exec python3 /app/autoscaler.py
#!/bin/bash

# Dataproc initialization script for additional packages
# This script installs any additional dependencies needed

set -e

echo "Starting Dataproc initialization..."

# Check if running on Debian 10 (Buster) and update sources if needed
if grep -q "buster" /etc/os-release 2>/dev/null; then
    echo "Detected Debian Buster, updating package sources to archive repositories..."
    
    # Backup original sources.list
    cp /etc/apt/sources.list /etc/apt/sources.list.backup
    
    # Update sources.list to use archive repositories for Debian Buster
    cat > /etc/apt/sources.list << 'EOF'
deb http://archive.debian.org/debian buster main
deb http://archive.debian.org/debian-security buster/updates main
EOF
    
    # Update without GPG check for archived repositories
    apt-get update -y --allow-releaseinfo-change || echo "Warning: Some repositories might be outdated"
else
    # For newer Debian versions, normal update should work
    apt-get update -y || echo "Warning: Package update failed, continuing with pre-installed packages"
fi

# Install additional Python packages if needed
# These packages are usually already available in Dataproc images
pip3 install --upgrade google-cloud-bigquery google-cloud-storage || echo "Warning: Failed to upgrade some packages, using pre-installed versions"

echo "Dataproc initialization completed successfully"

#!/bin/bash

# Dataproc initialization script for additional packages
# This script installs any additional dependencies needed

set -e

echo "Starting Dataproc initialization..."

# Update system packages
apt-get update -y

# Install additional Python packages if needed
pip3 install --upgrade google-cloud-bigquery google-cloud-storage

echo "Dataproc initialization completed successfully"

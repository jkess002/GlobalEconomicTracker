#!/bin/bash

set -e

echo "🔧 Removing old Docker components if present..."
sudo apt remove -y docker docker-engine docker.io containerd runc || true

echo "📦 Installing dependencies..."
sudo apt update
sudo apt install -y ca-certificates curl gnupg lsb-release

echo "🔐 Adding Docker GPG key..."
sudo mkdir -p /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | \
  sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg

echo "📋 Adding Docker repo to sources..."
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
  https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

echo "🔄 Updating package list..."
sudo apt update

echo "🐳 Installing Docker Engine, CLI, Buildx, and Compose..."
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

echo "🚀 Testing Docker installation..."
sudo docker run --rm hello-world

if [ -f "docker-compose.yml" ]; then
  echo "📦 docker-compose.yml found. Building containers..."
  sudo docker compose up --build -d
else
  echo "ℹ️ No docker-compose.yml found in the current directory."
fi

echo "✅ Docker setup complete!"

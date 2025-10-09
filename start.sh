#!/bin/sh

# This script will wait for the Docker daemon's API to be responsive.

echo "--- Checking Docker daemon API readiness ---"

# Loop until the 'docker info' command succeeds.
# We redirect output to /dev/null to keep the logs clean.
while ! docker info > /dev/null 2>&1; do
  echo "Waiting for Docker daemon API..."
  sleep 1 # wait for 1 second before checking again
done

echo "✅ Docker daemon API is ready."

# Pull the sandbox image
echo "--- Pulling worker image: akashcapro/codex-sandbox ---"
docker pull akashcapro/codex-sandbox:latest
echo "✅ Worker image pulled."

# Now, execute the main application command
echo "--- Starting Code Execution Service ---"
exec node src/index.js
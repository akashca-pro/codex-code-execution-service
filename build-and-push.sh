#!/bin/bash

# --- 1. Argument Check ---
if [ -z "$1" ] || [ -z "$2" ] || [ -z "$3" ]; then
  echo "❌ Error: You must provide BOTH an image name, dockerfile path and a version tag."
  echo "Usage: $0 <file-name> <image-name> <version>"
  exit 1
fi

# --- 2. Define Variables ---
IMAGE_NAME=$1
VERSION=$2
FULL_TAG="$IMAGE_NAME:$VERSION"
FILE_NAME=$3

echo "==========================================="
echo "  Starting Docker Build and Push"
echo "  Image Name: $IMAGE_NAME"
echo "  Version Tag: $VERSION"
echo " File name: $FILE_NAME "
echo "==========================================="

# --- 3. Build the Docker Image ---
echo "⚙️  Building Docker image: $FULL_TAG"
docker build --no-cache -f $FILE_NAME -t "$FULL_TAG" .

# Check if the build was successful
if [ $? -ne 0 ]; then
  echo "❌ Docker build failed."
  exit 1
fi

# --- 4. Push the Docker Image ---
echo "📤 Pushing Docker image: $FULL_TAG"
docker push "$FULL_TAG"

# Check if the push was successful
if [ $? -ne 0 ]; then
  echo "❌ Docker push failed."
  exit 1
fi

# --- 5. Completion ---
echo "✅ Successfully built and pushed: $FULL_TAG"
echo "==========================================="
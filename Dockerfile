# Stage 1: Get the Shaka Packager binary
# google/shaka-packager is the official repository for the binary
FROM google/shaka-packager:v3.6.0 AS packager_bin

# Stage 2: Your Python environment
FROM python:3.12-slim

# Install system dependencies (ffmpeg and ca-certificates for AWS/KMS communication)
RUN apt-get update && \
    apt-get install -y --no-install-recommends ffmpeg ca-certificates && \
    rm -rf /var/lib/apt/lists/* && \
    pip install --no-cache-dir pipenv

# Copy the packager binary from the first stage
# Placing it in /usr/bin makes it globally accessible in your CMD
COPY --from=packager_bin /usr/bin/packager /usr/bin/packager

WORKDIR /app

# Copy Pipfile and Pipfile.lock first for caching
COPY Pipfile Pipfile.lock* ./

# Install dependencies into the system Python
RUN pipenv install --system --deploy

# Copy your processing script
COPY task.py .

# Run the script
CMD ["python", "task.py"]
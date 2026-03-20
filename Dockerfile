
# Use Shaka Packager as the main base (musl-based, aarch64)
FROM google/shaka-packager:v3.6.0


# Install Python, boto3, pytest, ffmpeg, and other dependencies (Alpine Linux)
RUN apk add --no-cache python3 py3-pip py3-virtualenv ffmpeg ca-certificates py3-boto3 py3-pytest

WORKDIR /app




# Copy your processing script
COPY task.py .

# Run the script
CMD ["python3", "task.py"]
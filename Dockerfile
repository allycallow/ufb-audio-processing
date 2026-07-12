
# Use Shaka Packager as the main base (musl-based, aarch64)
FROM google/shaka-packager:v3.6.0


# Install Python, boto3, pytest, ffmpeg, and other dependencies (Alpine Linux)i
RUN apk add --no-cache python3 py3-pip py3-virtualenv ffmpeg ca-certificates py3-boto3 py3-pytest py3-requests

# sentry-sdk isn't packaged for Alpine, so install it via pip (--break-system-packages
# to opt out of PEP 668's externally-managed-environment guard)
RUN pip install --no-cache-dir --break-system-packages sentry-sdk==2.64.0

WORKDIR /app




# Copy your processing script
COPY task.py .

# Run the script
CMD ["python3", "task.py"]
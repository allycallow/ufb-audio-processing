FROM python:3.12-slim

# Install pipenv and ffmpeg dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends ffmpeg && \
    rm -rf /var/lib/apt/lists/* && \
    pip install --no-cache-dir pipenv

WORKDIR /app

# Copy Pipfile and Pipfile.lock first for caching
COPY Pipfile Pipfile.lock* ./

# Install dependencies into the system Python (no virtualenv)
RUN pipenv install --system --deploy

# Copy the application code
COPY convert.py .

# Run the script
CMD ["python", "convert.py"]
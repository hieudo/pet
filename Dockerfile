FROM python:3.11-slim

# Avoid writing .pyc files & enable stdout logging
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Copy only necessary files first (better caching)
COPY aggregator.py .
COPY test ./test

# Install pytest (for optional testing)
RUN pip install --no-cache-dir pytest

# Default command (can be overridden by docker-compose)
CMD ["python", "aggregator.py", "--help"]
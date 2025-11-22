FROM python:3.11-slim

# Environment settings (UTF-8 + no .pyc)
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    LANG=C.UTF-8 \
    LC_ALL=C.UTF-8 \
    PYTHONIOENCODING=UTF-8

# Set working directory inside container
WORKDIR /app

# Install system packages required for mysql-connector
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libmariadb-dev-compat libmariadb-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt

# Copy ONLY the needed project files
COPY scheduler.py .
COPY ingest.py .
COPY generate_session.py .

# Debug during build (you can remove later)
RUN echo "📁 Listing /app content during build:" \
  && ls -lah /app \
  && echo "Checking scheduler.py exists:" \
  && [ -f /app/scheduler.py ] && echo "scheduler.py FOUND" || echo "scheduler.py NOT FOUND"

# Create unprivileged user
RUN useradd --create-home appuser && chown -R appuser:appuser /app
USER appuser

# Start the job fetcher
CMD ["python", "scheduler.py"]
# Use the official Python image
FROM python:3.10-slim

# Install uv
RUN pip install uv

# Set working directory
WORKDIR /app

# Copy dependency files
COPY pyproject.toml uv.lock ./

# Install dependencies
RUN uv sync --frozen --no-install-project

# Copy the rest of the application
COPY . .

# Install the project
RUN uv sync --frozen

# Expose port for Prometheus metrics
EXPOSE 8000

CMD ["uv", "run", "app/scraper.py"]
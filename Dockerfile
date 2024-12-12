# Use the official Python image
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Install dependencies
COPY app/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy scraper script
COPY app/scraper.py /app/scraper.py


CMD [ "python3", "./scraper.py"]
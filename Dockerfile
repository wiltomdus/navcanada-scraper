# Use the official Python image
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Install dependencies
COPY app/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy scraper script
COPY app/scraper.py /app/scraper.py

# Install cron
RUN apt-get update && apt-get install -y cron

# Copy cron job
COPY cron/cronjob /etc/cron.d/cronjob

# Apply cron job permissions
RUN chmod 0644 /etc/cron.d/cronjob

# Create the log file
RUN touch /var/log/cron.log

# Start the cron service
CMD ["sh", "-c", "cron && tail -f /var/log/cron.log"]

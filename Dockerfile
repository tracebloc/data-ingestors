# Use Python 3.11 as base image
FROM --platform=linux/amd64 python:3.11


# Set working directory
WORKDIR /app

RUN apt-get update && apt-get install -y netcat-traditional

# Install data ingestor package
<for dev: RUN pip install git+https://<user-token>:x-oauth-basic@github.com/tracebloc/data-ingestors.git@develop#egg=tracebloc_ingestor>
<for prod: RUN pip install tracebloc_ingestor>

# Create necessary directories
RUN mkdir -p <directories>

# Copy the source code and requirements
<commands for copying all neccessary files and folder

# Set environment variables
ENV PYTHONPATH=/app

# Create an entrypoint script
COPY docker-entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# Set the entrypoint
ENTRYPOINT ["docker-entrypoint.sh"] 
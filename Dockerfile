# Use Python 3.9 as base image
FROM python:3.9


# Set working directory
WORKDIR /app

RUN apt-get update && apt-get install -y netcat-traditional

# Copy the source code and requirements
COPY requirements.txt requirements.txt
COPY examples/ examples/
COPY tracebloc_ingestor/ tracebloc_ingestor/
RUN mkdir data
# copy data (images, text, csv, json) inside data ingestion pod to start ingestion
# COPY <data> data/  uncomment and change data name
# Install Python dependencies
RUN --mount=type=cache,target=/root/.cache/pip pip install -r requirements.txt
# Create necessary directories
RUN mkdir -p storage/text_files storage/images

# Set environment variables
ENV PYTHONPATH=/app

# Create an entrypoint script
COPY docker-entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# Set the entrypoint
ENTRYPOINT ["docker-entrypoint.sh"] 
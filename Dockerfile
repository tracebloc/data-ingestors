# Use Python 3.9 as base image
FROM --platform=linux/amd64 python:3.9


# Set working directory
WORKDIR /app

# Copy the source code and requirements
COPY src/requirements.txt requirements.txt
COPY src/ src/

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Create necessary directories
RUN mkdir -p storage/text_files storage/images

# Set environment variables
ENV PYTHONPATH=/app

# Create an entrypoint script
COPY docker-entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# Set the entrypoint
ENTRYPOINT ["docker-entrypoint.sh"] 
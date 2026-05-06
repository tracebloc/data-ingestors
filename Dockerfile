# Use Python 3.11 as base image
FROM python:3.11

# Set working directory
WORKDIR /app

RUN apt-get update && apt-get install -y netcat-traditional

# Install data ingestor package
RUN pip install tracebloc_ingestor

# Copy the user's chosen ingestor (rename a template to ./ingestor.py before building) # train/test switch
COPY ingestor.py /app/ingestor.py


# Set environment variables
ENV PYTHONPATH=/app
# Disable GPU/CUDA usage
ENV CUDA_VISIBLE_DEVICES=-1
ENV TF_CPP_MIN_LOG_LEVEL=2

# Create an entrypoint script
COPY docker-entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# Set the entrypoint
ENTRYPOINT ["docker-entrypoint.sh"] 

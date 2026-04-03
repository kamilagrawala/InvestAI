# Use buster for better dependency support
FROM python:3.9-slim-buster

# Set working directory
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all python scripts
COPY *.py ./

# Environment variable for RabbitMQ host (defaults to 'rabbitmq')
ENV RABBITMQ_HOST=rabbitmq
ENV RABBITMQ_USER=admin
# This is the encrypted version of 'password'
ENV RABBITMQ_PASS=gAAAAABpz9Kay8WWKyW2XdJW6SD1k2U7dbxuyd_dyC-CbzvMSZpcehzc1TGadCcYlsQPOw0J_iZK8NlMuLDqMwYIPPil-F9M9A==

# Run the consumer
CMD ["python", "-u", "consumer.py"]

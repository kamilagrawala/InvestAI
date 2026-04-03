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
ENV RABBITMQ_PASS=password

# Run the consumer
CMD ["python", "-u", "consumer.py"]

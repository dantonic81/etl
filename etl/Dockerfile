# Use a build stage to install dependencies
FROM python:3.8-slim AS builder
# Set the working directory to /app
WORKDIR /app
COPY . /app
# Install any needed packages specified in requirements.txt
RUN pip install --trusted-host pypi.python.org -r etl/requirements.txt

# Use a smaller image for the final stage
FROM python:3.8-slim
WORKDIR /app
COPY --from=builder /usr/local/lib/python3.8/site-packages /usr/local/lib/python3.8/site-packages
COPY --from=builder /app /app

# Define environment variable
ENV PYTHONPATH "${PYTHONPATH}:/app"

# Run etl.py when the container launches
CMD ["python", "etl/main.py"]

# Use an official Python runtime as a parent image
FROM python:3.8-slim

# Set the working directory to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install --trusted-host pypi.python.org -r etl/requirements.txt

# Define environment variable
ENV PYTHONPATH "${PYTHONPATH}:/app"

# Run etl.py when the container launches
CMD ["python", "etl/main.py"]
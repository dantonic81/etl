# ETL Solution for Marketing Campaign Data

This repository contains a simple ETL (Extract, Transform, Load) solution for processing marketing campaign data from URLs and storing it in a PostgreSQL database. The solution is designed to be run as a Docker-compose project, with separate services for the ETL process and the PostgreSQL database.

## Project Structure

- `etl/`: Directory containing the ETL script and other necessary files.
- `data/`: Directory containing the raw URLs data in the `raw_urls.csv` file.
- `docker-compose.yml`: Docker Compose configuration file for setting up services.
- `tests/`: Directory containing unit tests for the ETL process.

## Prerequisites

- Docker
- Python
- PostgreSQL

## Running the ETL Solution

1. Unzip the provided archive
2. Navigate to the project directory:

    ```bash
    cd path-to-your-unzipped-archive
    ```

3. Build and run the Docker-compose project:

    ```bash
    docker-compose up --build
    ```

4. The ETL process will run, parsing the data from `raw_urls.csv` and inserting it into the PostgreSQL database.

## Configuration

- PostgreSQL database configuration is specified in the `.env` file.
- Adjust the Docker-compose and Python scripts based on your project structure if needed.

## Testing

Run the unit tests using pytest:

   ```bash
   pytest
   ```

## Additional Notes

- The ETL script (`main.py`) uses the `urllib.parse` module to parse URLs. If your requirements involve making HTTP requests, consider using dedicated HTTP libraries like `requests` or `httpx`.
- Customize the ETL script further based on your specific data processing requirements.

## Bonus Features

- The ETL solution includes basic error handling, logging, and unit tests for enhanced robustness and maintainability.

## Dependencies

- Python dependencies are specified in the `requirements.txt` file.
- The required Python packages will be installed during the Docker build process.

## Best Practices for Production

Consider the following best practices that are commonly used in production environments but not necessarily followed in this solution:

Secrets Management: In a production environment, sensitive information such as database credentials should be managed using secure and dedicated solutions like HashiCorp Vault or Docker Secrets.

Monitoring and Logging: Implement a robust monitoring and logging solution to track the performance, errors, and overall health of the ETL process in a production environment.

Container Orchestration: Utilize container orchestration tools like Kubernetes or Docker Swarm for better scalability, deployment management, and resource utilization.

Data Validation and Cleaning: Enhance data validation and cleaning processes to handle edge cases and ensure data accuracy and consistency in production.

Parallel Processing: Implement parallel processing to improve the speed and efficiency of the ETL process, especially when dealing with large datasets.

Automated Testing: Extend test coverage with automated integration tests and end-to-end tests to catch potential issues before deployment.
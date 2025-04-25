# Scalable ETL Pipeline (MySQL to PostgreSQL)

This project implements a simple, scalable, and generic ETL pipeline to copy data from a MySQL source database to a PostgreSQL target database using Python, Docker, and Apache Airflow for orchestration and parallelism.

## Features

- **Dockerized**: The pipeline logic runs within a Docker container, ensuring consistency across environments.  
- **Generic Connectors**: Designed with a connector pattern (`util.py`) to easily extend support for different data sources (databases, APIs, files) and targets.  
- **Parallel Processing**: Leverages Apache Airflow's `DockerOperator` to run multiple data copying tasks concurrently for improved performance.  
- **Configurable**: Database connection details and tables to process are managed via configuration files and command-line arguments.  
- **Idempotent (Table-level)**: Each Airflow task processes a specific group of tables independently, making it easier to retry failures at a granular level.

## Prerequisites

- Docker and Docker Compose (recommended for easier setup)  
- Python 3.8+  
- Git  
- Apache Airflow (if running with DAGs)  

## Project Structure

```
├── app.py              # Main application entry point (parses args, orchestrates copy)
├── config.py           # Database connection details configuration
├── util.py             # Database connector implementations and general utilities
├── tables_list         # CSV file listing tables and load status
├── requirements.txt    # Python dependencies
├── Dockerfile          # Builds the Docker image for the pipeline application
```

`read.py` and `write.py` (from previous iterations) have been refactored and their core logic integrated into the connector classes within `util.py`. These files can now be removed or are empty placeholders.

---

## Setup

### Clone the Repository

```bash
git clone <your_project_repo_url>
cd <your_project_directory>
```

*(Replace `<your_project_repo_url>` and `<your_project_directory>` with your actual repository details)*

### Clone Source Database Data

The source MySQL database will be initialized with data from the retail_db repository.

```bash
git clone https://github.com/fmWaithaka/retail_db.git
```

Make a note of the path to the cloned `retail_db` directory on your host machine.

### Create a Docker Network

This network allows your application container to communicate with the database containers using their service names.

```bash
docker network create my_pipeline_network
```

### Set up Source MySQL Database

Run the MySQL container, mounting the cloned `retail_db` directory into the container's `/docker-entrypoint-initdb.d/`. This will automatically execute the `create_db.sql` script to create tables and load data on the first run.

> **Important**: Ensure you have removed the PostgreSQL-specific `.sql` files (`create_db_tables_pg.sql`, `load_db_tables_pg.sql`) from your host's `retail_db` directory before running this command, as they can cause the MySQL init script to fail.

```bash
docker run --name mysql-container \
--network my_pipeline_network \
-v path_to_your_db\retail_db:/docker-entrypoint-initdb.d/ \
-e MYSQL_ROOT_PASSWORD=root \
-e MYSQL_DATABASE=retail_db \
-e MYSQL_USER=retail_user \
-e MYSQL_PASSWORD=itversity \
-p 3306:3306 \
-d mysql:8.0
```

*(Replace `path_to_your_db\retail_db` with the actual path on your host)*

### Set up Target PostgreSQL Database

Run the PostgreSQL container. It will be empty initially.

```bash
docker run --name postgres-db \
--network my_pipeline_network \
-e POSTGRES_PASSWORD=itversity \
-e POSTGRES_USER=retail_user \
-e POSTGRES_DB=retail_db \
-p 5432:5432 \
-d postgres
```

### Update `config.py` (if necessary)

Verify that `config.py` uses the correct container names (`mysql-container` and `postgres-db`) for `DB_HOST` in the dev environment.

```python
# config.py
DB_DETAILS = {
    'dev': {
        'SOURCE_DB': {
            'DB_TYPE': 'mysql',
            'DB_HOST': 'mysql-container', # Should match the container name
            'DB_NAME': 'retail_db',
            'DB_USER': os.environ.get('SOURCE_DB_USER'),
            'DB_PASS': os.environ.get('SOURCE_DB_PASS')
        },
        'TARGET_DB': {
            'DB_TYPE': 'postgres',
            'DB_HOST': 'postgres-db', # Should match the container name
            'DB_NAME': 'retail_db',
            'DB_USER': os.environ.get('TARGET_DB_USER'),
            'DB_PASS': os.environ.get('TARGET_DB_PASS')
        }
    }
}
```

---

## Build the Pipeline Docker Image

Build the Docker image for your application code.

```bash
docker build -t data-pipeline:latest .
```

---

## Running the Pipeline

### Manually (without Airflow)

You can run the pipeline directly using `docker run`. This is useful for testing or one-off executions. The example below copies all tables specified in `tables_list`.

```bash
docker run --name data-pipeline \
-v path_to_your_project:/app \
-it \
-e SOURCE_DB_USER=retail_user \
-e SOURCE_DB_PASS=itversity \
-e TARGET_DB_USER=retail_user \
-e TARGET_DB_PASS=itversity \
--network my_pipeline_network \
--entrypoint python \
data-pipeline \
app.py dev all
```

To copy specific tables (e.g., only `customers` and `orders`), replace `all` with a comma-separated list:

```bash
docker run ... data-pipeline app.py dev customers,orders
```

---

### With Apache Airflow

#### Ensure Airflow Setup

Make sure you have a running Airflow environment (local, Docker Compose, Kubernetes, etc.) and that it can communicate with your Docker daemon (often requires mounting `/var/run/docker.sock` into the Airflow worker/scheduler containers).

#### Place the DAG File

Copy the `data_pipeline_docker.py` file into your Airflow `dags` folder.

```python
# dags/data_pipeline_docker.py
# Full DAG code here...
```

Airflow UI: The DAG should appear in the Airflow UI. You can trigger it manually or let the schedule run it. You will see multiple DockerOperator tasks running concurrently.

---

## Extensibility

The refactored `util.py` uses a connector pattern with factory functions (`create_source_connector`, `create_target_connector`). To add support for a new data source (like an API) or a new target (like writing to S3 or a file format):

- Create a new connector class (e.g., `APISourceConnector`, `S3TargetConnector`) inheriting from `SourceConnector` or `TargetConnector`.
- Implement the required methods (`connect`, `read_table` or `load_table`).
- Update the corresponding factory function (`create_source_connector` or `create_target_connector`) in `util.py` to include an `elif` block that checks for the new `DB_TYPE` (or `SOURCE_TYPE`/`TARGET_TYPE`) and returns an instance of your new connector class.
- Update `config.py` to include configuration details for the new source/target type.
- Update `requirements.txt` with any new Python libraries needed for the new connector (e.g., `requests` for an API).

---

## Troubleshooting

**MySQL access denied or Connection Errors:**

- Verify the environment variables (`SOURCE_DB_USER`, `SOURCE_DB_PASS`, `TARGET_DB_USER`, `TARGET_DB_PASS`) are correctly passed to the Docker container.
- Ensure the Docker containers (`mysql-container`, `postgres-db`, `data-pipeline`) are all on the same Docker network (`my_pipeline_network`).
- Check that the `DB_HOST` values in `config.py` (`mysql-container`, `postgres-db`) match the container names.
- Check the container logs (`docker logs <container_name>`) for specific error details.

**Container Stops Immediately After Start:**

- For the database containers, this often means an initialization script failed. Check `docker logs <container_name>`.
- For MySQL, ensure only MySQL-specific `.sql` files are in `/docker-entrypoint-initdb.d/`.
- For the pipeline container, check `docker logs data-pipeline` for Python traceback errors.

**Airflow DockerOperator Errors:**

- Verify the `docker_url` in the DAG is correct and the Airflow worker/scheduler can access the Docker daemon.
- Ensure the `mounts` source path is correct and accessible from where the Docker container is being launched by Airflow.
- Check the Airflow task logs for the specific error output from the Docker container execution.


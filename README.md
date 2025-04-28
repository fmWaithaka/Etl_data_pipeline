# Scalable ETL Pipeline (MySQL to PostgreSQL)

This project implements a robust, scalable, and generic ETL pipeline to copy data from a MySQL source database to a PostgreSQL target database. It leverages Python for the core ETL logic, Docker for containerization, and Apache Airflow for orchestration, parallel execution, and incremental loading using a watermark strategy.

![Airflow DAG Pipeline](Screenshot/Dag%20pipeline.png)

## Features

- **Dockerized**: The pipeline application runs within a Docker container, ensuring a consistent and isolated execution environment.
- **Generic Connectors**: Designed with a flexible connector pattern (util.py) that makes it easy to extend support for various data sources (databases, APIs, files) and targets.
- **Parallel Processing**: Utilizes Apache Airflow's DockerOperator to execute multiple data copying tasks concurrently, significantly improving performance for large datasets.
- **Incremental Loading (Watermarking)**: Implements a watermark strategy (using auto-incrementing IDs or timestamps) to copy only new or changed data since the last successful run, optimizing data movement and reducing load times.
- **Airflow Orchestration**: Managed by an Apache Airflow DAG for scheduling, monitoring, retries, and state management (via Airflow Variables).
- **Configurable**: Database connection details, tables to process, and incremental loading settings are managed via configuration files (config.py, tables_list) and Airflow Variables.

## Prerequisites

- Docker and Docker Compose (recommended for easier local setup)
- Python 3.8+
- Git
- Apache Airflow environment (with DockerOperator enabled and access to the Docker daemon)

## Project Structure

```
├── app.py              # Main application entry point (parses args, orchestrates copy)
├── config.py           # Database connection details configuration
├── util.py             # Database connector implementations, watermark utilities, and general helpers
├── tables_list         # CSV file listing tables, load status, and watermark configuration
├── requirements.txt    # Python dependencies
├── Dockerfile          # Builds the Docker image for the pipeline application
└── dags/               # Airflow DAG file(s)
    └── data_pipeline_with_incremental.py # Airflow DAG for parallel incremental execution
```
Here is the enhanced and professional version of your **Setup** section, including installation of `requirements.txt` for both Windows and Linux:

---

## Setup

### Clone the Repository

```bash
git clone https://github.com/fmWaithaka/Etl_data_pipeline.git
cd Etl_data_pipeline
```

#### Create and Activate a Virtual Environment (Recommended)

**For Linux/macOS:**

```bash
python3 -m venv venv
source venv/bin/activate
```

**For Windows:**

```bash
python -m venv venv
venv\Scripts\activate
```

#### Install the Project Dependencies

After activating the virtual environment, install all required packages:

```bash
pip install -r requirements.txt
```
---

### Clone Source Database Data

The source MySQL database will be initialized with data from the retail_db repository.

```bash
git clone https://github.com/fmWaithaka/retail_db.git
```

Make a note of the path to the cloned retail_db directory on your host machine (e.g., `C:\Users\your_name\Downloads\retail_db`).

### Create a Docker Network

This network allows your application container to communicate with the database containers using their service names.

```bash
docker network create my_pipeline_network
```

### Set up Source MySQL Database

Run the MySQL container, mounting the cleaned retail_db directory:

```bash
docker run --name mysql-container \
--network my_pipeline_network \
-v C:\Users\your_name\Downloads\retail_db:/docker-entrypoint-initdb.d/ \
-e MYSQL_ROOT_PASSWORD=root \
-e MYSQL_DATABASE=retail_db \
-e MYSQL_USER=retail_user \
-e MYSQL_PASSWORD=itversity \
-p 3306:3306 \
-d mysql:8.0
```
(Replace `C:\Users\your_name\Downloads\retail_db` with your actual path.)

### Set up Target PostgreSQL Database

Run the PostgreSQL container:

```bash
docker run --name postgres-db \
--network my_pipeline_network \
-e POSTGRES_PASSWORD=itversity \
-e POSTGRES_USER=retail_user \
-e POSTGRES_DB=retail_db \
-p 5432:5432 \
-d postgres
```

### Update `config.py`

Verify `config.py` uses the correct container names:

```python
# config.py
import os

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

### Build the Pipeline Docker Image

```bash
docker build -t data-pipeline:latest .
```

### Airflow Setup

- Ensure you have a running Airflow environment.
- Ensure Airflow workers/schedulers can access the Docker daemon (`/var/run/docker.sock`).
- Ensure Airflow workers/schedulers can access the host path specified in the DAG mounts.
- For detailed guidance on using the Airflow Python client, refer to the [official Apache Airflow client documentation](https://github.com/apache/airflow-client-python).
## Running the Pipeline

### Manually (Full Load)

```bash
docker run --name data-pipeline \
-v \project_path\Etl_data_pipeline:/app \
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
(Replace the `\project_path\Etl_data_pipeline` with your actual one.)

### With Apache Airflow (Parallel & Incremental)

#### Place the DAG File

Copy `data_pipeline_with_incremental.py` into your Airflow dags folder.

#### Airflow Variables

Create the following variables (initial value `None`):

- `last_watermark_customers`
- `last_watermark_orders`
- `last_watermark_order_items`

#### Trigger the DAG

The DAG should now appear in Airflow UI for manual triggering or scheduled runs.

---

### Simulating New Data for Incremental Loading

To test the incremental loading functionality, you must insert **new records** into the source MySQL database **after** an initial full load has completed and Airflow Variables have been set.

Use the following SQL script to simulate new `customers`, `orders`, and `order_items`:

```sql
-- SQL script to insert new data for incremental loading testing

-- Start a transaction to ensure atomicity
START TRANSACTION;

-- Insert new customers (check for duplicates based on email)
INSERT INTO customers (customer_fname, customer_lname, customer_email, customer_password, customer_street, customer_city, customer_state, customer_zipcode)
SELECT 'New', 'CustomerOne', 'new.customer1@example.com', 'securepass1', '789 Pine Rd', 'Villagetown', 'TX', '75001'
WHERE NOT EXISTS (
    SELECT 1 FROM customers WHERE customer_email = 'new.customer1@example.com'
);

INSERT INTO customers (customer_fname, customer_lname, customer_email, customer_password, customer_street, customer_city, customer_state, customer_zipcode)
SELECT 'Another', 'UserTwo', 'another.user2@example.com', 'mypassword', '101 Maple Ln', 'Hamlet City', 'FL', '32003'
WHERE NOT EXISTS (
    SELECT 1 FROM customers WHERE customer_email = 'another.user2@example.com'
);

-- Retrieve the customer IDs for the newly inserted customers
SET @customer_id_one = (SELECT customer_id FROM customers WHERE customer_email = 'new.customer1@example.com');
SET @customer_id_two = (SELECT customer_id FROM customers WHERE customer_email = 'another.user2@example.com');

-- Insert new orders associated with the new customers
INSERT INTO orders (order_date, order_customer_id, order_status)
VALUES (NOW(), @customer_id_one, 'PENDING');

SET @order_id_one = LAST_INSERT_ID(); -- Get the auto-generated order_id for the first order

INSERT INTO orders (order_date, order_customer_id, order_status)
VALUES (NOW(), @customer_id_two, 'COMPLETE');

SET @order_id_two = LAST_INSERT_ID(); -- Get the auto-generated order_id for the second order

-- Insert new order items linked to the new orders
INSERT INTO order_items (order_item_order_id, order_item_product_id, order_item_quantity, order_item_subtotal, order_item_product_price)
VALUES (
    @order_id_one,
    4, -- Example product_id
    2,
    (SELECT 2 * product_price FROM products WHERE product_id = 4),
    (SELECT product_price FROM products WHERE product_id = 4)
);

INSERT INTO order_items (order_item_order_id, order_item_product_id, order_item_quantity, order_item_subtotal, order_item_product_price)
VALUES (
    @order_id_two,
    2, -- Example product_id
    1,
    (SELECT 1 * product_price FROM products WHERE product_id = 2),
    (SELECT product_price FROM products WHERE product_id = 2)
);

-- Optional: Verify inserts before committing
-- SELECT * FROM customers WHERE customer_email LIKE '%example.com';
-- SELECT * FROM orders WHERE order_id IN (@order_id_one, @order_id_two);
-- SELECT * FROM order_items WHERE order_item_order_id IN (@order_id_one, @order_id_two);

-- Finalize the transaction
COMMIT;
```

> **Note:**  
> Execute this script against your **source MySQL database** only after:
> - The initial full data load has been completed successfully.
> - The incremental load mechanism (Airflow Variables, Watermarks) has been properly configured.

This ensures your **incremental ETL pipeline** will detect and process only the newly inserted records during the next scheduled run.

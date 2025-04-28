from __future__ import annotations

import pendulum
import logging

from airflow.models.dag import DAG
from airflow.models.variable import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

log = logging.getLogger(__name__) # Configure logging

# Define common parameters for your DockerOperator tasks
docker_image = 'data-pipeline:latest'
network_name = 'my_pipeline_network'
app_path_in_container = '/app' # The path inside the container where your code is
# IMPORTANT: Replace with the actual path to your DataPipeline folder on your Airflow host
# This path must be accessible by the Docker daemon used by Airflow workers.
host_code_path = '/root/etl-pipeline/Etl_data_pipeline'

# Environment variables to pass to each container (excluding watermark)
base_env_vars = {
    'SOURCE_DB_USER': 'retail_user',
    'SOURCE_DB_PASS': 'itversity',
    'TARGET_DB_USER': 'retail_user',
    'TARGET_DB_PASS': 'itversity',
}

# Volume mounts for each container
mounts = [
    Mount(source=host_code_path, target=app_path_in_container, type='bind'),
]

# --- Watermark Management Functions (Used by PythonOperators) ---

def get_last_watermark(**context):
    """
    Retrieves the last watermark value for a table from Airflow Variables.
    Uses context to get table_name passed via op_kwargs.
    """
    table_name = context['params']['table_name']
    variable_key = f"last_watermark_{table_name.lower()}"
    # Default value is None. For initial run, this will be None, triggering full load in app.py.
    last_watermark = Variable.get(variable_key, default_var=None)
    log.info(f"get_last_watermark task: Retrieved value for variable '{variable_key}': {last_watermark} (Type: {type(last_watermark)})")

    log.info(f"Retrieved last watermark for {table_name}: {last_watermark}")
    return last_watermark # Push the retrieved value to XCom

def set_new_watermark(**context):
    """
    Sets the new watermark value for a table in Airflow Variables.
    Pulls the new value from the preceding DockerOperator task's XCom.
    """
    table_name = context['params']['table_name']
    variable_key = f"last_watermark_{table_name.lower()}" # Define variable_key unconditionally
    docker_task_id = f'copy_{table_name}' # Assuming the preceding DockerOperator task_id
    raw_output = context['ti'].xcom_pull(task_ids=docker_task_id, key='return_value') # Pull output from Docker task

    new_watermark_value = None
    if raw_output:
        prefix = f"NEW_WATERMARK_{table_name.upper()}="
        if isinstance(raw_output, str) and raw_output.startswith(prefix):
             new_watermark_value_str = raw_output[len(prefix):].strip()
             new_watermark_value = new_watermark_value_str
             log.info(f"Extracted new watermark value: {new_watermark_value}")
        else:
            log.warning(f"Docker task {docker_task_id} output format unexpected or empty.")
            log.warning(f"Raw output: {raw_output}")
    else:
        log.warning(f"No output found in XCom for task {docker_task_id}. Watermark will not be updated.")

    if new_watermark_value is not None:
        Variable.set(variable_key, str(new_watermark_value)) # Store as string
        log.info(f"Updated watermark for {table_name} to {new_watermark_value}")
    else:
        log.warning(f"No valid new watermark value found for {table_name}. Variable '{variable_key}' not updated.")

# --- DAG Definition ---

with DAG(
    dag_id='data-pipeline-parallel-incremental',
    schedule='0 0 * * *', # Example: Run daily at midnight UTC
    start_date=pendulum.datetime(2025, 4, 28, tz="EAT"), # Set your appropriate start date
    catchup=False,
    tags=['docker', 'etl', 'parallel', 'incremental'],
    concurrency=4, # Limit concurrent tasks within this DAG run
    max_active_tasks=4 # Limit active tasks across all DAG runs for this DAG
) as dag:

    # Dummy tasks for visualization
    start = DummyOperator(task_id='start_pipeline')
    end = DummyOperator(task_id='end_pipeline')

    # Define tables that support incremental loading via watermark
    # This list should correspond to entries in your tables_list file
    incremental_tables = [
        'customers',
        'orders',
        'order_items',
    ]

    # Define tables that will always be full loaded (no watermark)
    full_load_tables = [
        'departments',
        'categories',
        'products',
    ]

    # Create tasks for incremental tables (get watermark -> copy -> update watermark)
    incremental_task_chains_start = [] # List to hold the first task of each chain
    incremental_task_chains_end = [] # List to hold the last task of each chain

    for table_name in incremental_tables:
        # Task to get the last watermark
        get_last_watermark_task = PythonOperator(
            task_id=f'get_last_watermark_{table_name}',
            python_callable=get_last_watermark,
            # Pass table_name as a parameter to the python_callable
            params={'table_name': table_name},
            do_xcom_push=True, # Push the watermark value to XCom
            dag=dag,
        )

        # Task to run the Docker container for this table
        copy_table_task = DockerOperator(
            task_id=f'copy_{table_name}',
            image=docker_image,
            auto_remove='success',
            # Pass the last watermark value as an environment variable
            # The value is pulled from the get_last_watermark_task's XCom
            environment={
                **base_env_vars, # Include base env vars
                # Construct the environment variable name and pull the value from XCom
                # Ensure the pulled value is explicitly cast to str to avoid potential issues
                f'LAST_WATERMARK_{table_name.upper()}': "{{ task_instance.xcom_pull(task_ids='get_last_watermark_" + table_name + "', key='return_value') | string }}"
            },
            mounts=mounts,
            network_mode=network_name,
            entrypoint='python',
            # Pass only the current table name to app.py
            # app.py will read the watermark env var internally
            command=['/app/app.py', 'dev', table_name],
            docker_url='unix:///var/run/docker.sock',
            do_xcom_push=True, # Important: Enable XCom push to capture output
            dag=dag,
        )

        # Task to update the watermark variable
        update_watermark_task = PythonOperator(
            task_id=f'update_watermark_{table_name}',
            python_callable=set_new_watermark,
             # Pass table_name as a parameter to the python_callable
            params={'table_name': table_name},
            # The set_new_watermark function will pull the new value from XCom internally
            dag=dag,
        )

        # Define dependencies for this table's tasks
        get_last_watermark_task >> copy_table_task >> update_watermark_task

        # Add the start and end tasks of this chain to the lists
        incremental_task_chains_start.append(get_last_watermark_task)
        incremental_task_chains_end.append(update_watermark_task)

    # Create tasks for full load tables (if any)
    full_load_tasks = []
    # Grouping full load tables into a single task is still possible if desired,
    # or create one task per full load table for finer granularity.
    # Example: One task for all full load tables
    if full_load_tables:
        full_load_combined_task = DockerOperator(
            task_id='copy_full_load_tables',
            image=docker_image,
            auto_remove='success',
            environment=base_env_vars, # No watermark env vars needed
            mounts=mounts,
            network_mode=network_name,
            entrypoint='python',
            # Pass the list of full load tables to app.py
            command=['/app/app.py', 'dev', ','.join(full_load_tables)],
            docker_url='unix:///var/run/docker.sock',
            dag=dag,
        )
        full_load_tasks.append(full_load_combined_task)

    # Define overall DAG dependencies
    # The 'start' dummy runs before all copy tasks (both incremental chains and full load)
    # All copy tasks (the end of incremental chains and full load tasks) must complete before the 'end' dummy
    start >> incremental_task_chains_start
    if full_load_tasks:
        start >> full_load_tasks
        incremental_task_chains_end >> end
        full_load_tasks >> end
    else:
         incremental_task_chains_end >> end

    # If there are no incremental tasks, only define dependencies for full load
    if not incremental_tables and full_load_tasks:
        start >> full_load_tasks >> end
    # If there are no tasks at all (shouldn't happen in this case), handle appropriately
    if not incremental_tables and not full_load_tables:
         start >> end
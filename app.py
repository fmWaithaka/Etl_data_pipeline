import logging
import sys
import argparse
import os
from typing import Optional, List, Tuple, Any, Dict

from util import load_db_details, get_tables, create_source_connector, create_target_connector, find_max_watermark_value

# --- Logging Setup ---
LOG_FILE = "dataPipeline.log"
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a",
    force=True
)
# Add console handler for visibility during execution
console_handler = logging.StreamHandler(sys.stdout) # Use sys.stdout explicitly
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
# Avoid adding handler multiple times if script is reloaded
if not any(isinstance(handler, logging.StreamHandler) for handler in logging.getLogger('').handlers):
    logging.getLogger('').addHandler(console_handler)
# --- End Logging Setup ---


def main():
    """
    Main function to run the ETL pipeline.
    Parses arguments, loads configuration, and processes specified tables.
    Handles incremental loading based on passed watermark values.
    """
    parser = argparse.ArgumentParser(description="Run a data pipeline for specified tables.")
    parser.add_argument("env", help="The environment configuration to use (e.g., 'dev').")
    parser.add_argument(
        "table_arg",
        help="Comma-separated list of tables to process, or 'all' to process all tables from the list file."
    )
    parser.add_argument(
        "--tables-list-path",
        default="tables_list", # Default file name
        help="Path to the file containing the list of tables if table_arg is 'all'."
    )

    # Note: Watermark values are expected to be passed via environment variables
    # by the orchestrator (Airflow). The script reads them here.

    args = parser.parse_args()

    env = args.env
    table_arg = args.table_arg
    tables_list_path = args.tables_list_path

    logging.info(f"Starting pipeline for environment '{env}' and tables '{table_arg}'...")

    logging.info("Loading DB configuration...")
    db_config = load_db_details(env)
    if not db_config:
        logging.error(f"Failed to load configuration for environment '{env}'.")
        sys.exit(1)

    source_db_config = db_config.get("SOURCE_DB")
    target_db_config = db_config.get("TARGET_DB")
    if not (source_db_config and target_db_config):
        logging.error("Configuration is missing 'SOURCE_DB' or 'TARGET_DB' sections.")
        sys.exit(1)

    # Create source and target connectors
    logging.info("Creating source connector...")
    source_connector = create_source_connector(source_db_config)
    if not source_connector:
        logging.error("Failed to create source connector. Exiting.")
        sys.exit(1)

    logging.info("Creating target connector...")
    target_connector = create_target_connector(target_db_config)
    if not target_connector:
        logging.error("Failed to create target connector. Exiting.")
        sys.exit(1)


    logging.info("Retrieving table list to process...")
    # get_tables now reads watermark columns
    tables_df = get_tables(tables_list_path, table_arg)
    if tables_df is None or tables_df.empty:
        logging.error("No tables found to process based on arguments.")
        sys.exit(1)

    logging.info(f"Tables to process: {tables_df['table_name'].tolist()}")

    # --- Main Processing Loop ---
    for index, row in tables_df.iterrows(): # Iterate over DataFrame rows
        table_name = row['table_name']
        watermark_column = row.get('watermark_column') # Use .get() for safety
        watermark_type = row.get('watermark_type')

        logging.info(f"Processing table: {table_name}")

        # --- Incremental Loading Logic ---
        last_watermark_value = None
        if watermark_column:
            # Read the last watermark value from an environment variable
            # The environment variable name format is assumed to be LAST_WATERMARK_<TABLE_NAME_UPPERCASE>
            env_var_name = f"LAST_WATERMARK_{table_name.upper()}"
            last_watermark_str = os.environ.get(env_var_name)

            if last_watermark_str is None or last_watermark_str.lower() == 'none':
                 logging.info(f"No last watermark found for {table_name} (env var {env_var_name} is None or 'None'). Performing full initial load.")
                 # If no last watermark value is found (None) or it's the string 'None',
                 # treat it as a full initial load.
                 watermark_column = None # Ensure full load query is built
                 last_watermark_value = None
            else:
                 try:
                     # Attempt to convert the string value based on watermark_type
                     if watermark_type == 'id':
                         last_watermark_value = int(last_watermark_str)
                         logging.info(f"Retrieved last ID watermark for {table_name}: {last_watermark_value}")
                     elif watermark_type == 'timestamp':
                         # Depending on the exact format, you might need more sophisticated parsing
                         last_watermark_value = last_watermark_str # Pass as string for SQL comparison
                         logging.info(f"Retrieved last Timestamp watermark for {table_name}: {last_watermark_value}")
                     else:
                         logging.warning(f"Unknown watermark_type '{watermark_type}' for table {table_name}. Performing full load.")
                         watermark_column = None # Disable incremental for this table
                         last_watermark_value = None
                 except ValueError:
                      logging.error(f"Failed to convert last watermark value '{last_watermark_str}' for table {table_name} with type '{watermark_type}'. Performing full load.", exc_info=True)
                      watermark_column = None # Disable incremental for this table
                      last_watermark_value = None
        else:
             logging.info(f"No watermark column defined for table {table_name}. Performing full load.")
        # --- End Incremental Loading Logic ---


        try:
            logging.info(f"Reading data from source for table: {table_name}")
            # Pass watermark details to the read_table method
            data, column_names = source_connector.read_table(
                table_name=table_name,
                watermark_column=watermark_column,
                last_watermark_value=last_watermark_value
            )

            if data is None or len(data) == 0:
                logging.warning(f"No new data found in source for table: {table_name}. Skipping load.")
                # If no data is found, the watermark doesn't change.
                # We still need to signal this to the orchestrator if necessary,
                # or the orchestrator assumes the last watermark is unchanged if no new one is output.
                # For simplicity, we'll only output a new watermark if data was processed.
                continue

            logging.info(f"Loading data into target for table: {table_name}")
            target_connector.load_table(table_name, data, column_names)
            logging.info(f"Successfully processed table: {table_name}")

            # --- Calculate and Output New Watermark ---
            if watermark_column and column_names and data:
                 new_watermark_value = find_max_watermark_value(data, column_names, watermark_column)
                 if new_watermark_value is not None:
                      # Output the new watermark value in a parsable format for Airflow
                      # Using print to stdout is common for Airflow to capture via XComs
                      print(f"NEW_WATERMARK_{table_name.upper()}={new_watermark_value}")
                      logging.info(f"Outputted new watermark for {table_name}: {new_watermark_value}")
                 else:
                      logging.warning(f"Could not determine new watermark for {table_name} despite data being processed.")
            # --- End Calculate and Output New Watermark ---


        except Exception as e:
            logging.error(f"Error processing table {table_name}: {e}", exc_info=True)
            # Decide whether to continue or exit on error.
            # Continuing allows other tables to potentially succeed.
            pass # Continue loop after logging error

    logging.info("Pipeline processing loop completed.")

    # Close connections managed by connectors
    if source_connector:
        source_connector.close()
    if target_connector:
        target_connector.close()


if __name__ == "__main__":
    try:
        main()
        logging.info("Pipeline finished successfully.")
    except Exception as main_e:
        # Catch any unhandled exceptions from main
        logging.critical(f"An unhandled error occurred during pipeline execution: {main_e}", exc_info=True)
        sys.exit(1) # Exit with a non-zero code to indicate failure
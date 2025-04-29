import logging
import sys
import argparse
import os
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
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
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
        default="tables_list",
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

    logging.info("Creating source connector...")
    source_connector = create_source_connector(source_db_config) # Call factory from util
    if not source_connector:
        logging.error("Failed to create source connector. Exiting.")
        sys.exit(1)

    logging.info("Creating target connector...")
    target_connector = create_target_connector(target_db_config) # Call factory from util
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
        if watermark_column and str(watermark_column).strip() != '':
            # Read the last watermark value from an environment variable
            # The environment variable name format is assumed to be LAST_WATERMARK_<TABLE_NAME_UPPERCASE>
            env_var_name = f"LAST_WATERMARK_{table_name.upper()}"
            last_watermark_str = os.environ.get(env_var_name)

            # Correctly handle the case where the environment variable is None, '', or the string 'None'
            if last_watermark_str is not None and last_watermark_str.lower() != 'none' and last_watermark_str.strip() != '':
                 try:
                     if watermark_type == 'id':
                         last_watermark_value = int(last_watermark_str)
                         logging.info(f"Retrieved last ID watermark for {table_name}: {last_watermark_value}")
                     elif watermark_type == 'timestamp':
                         # For timestamp, pass the string directly to the SQL query
                         # Ensure the format is compatible with MySQL timestamp comparison
                         last_watermark_value = last_watermark_str
                         logging.info(f"Retrieved last Timestamp watermark for {table_name}: '{last_watermark_value}'")
                     else:
                         logging.warning(f"Unknown watermark_type '{watermark_type}' for table {table_name}. Performing full load.")
                         # If type is unknown, disable incremental for this table
                         watermark_column = None
                         last_watermark_value = None
                 except ValueError:
                      logging.error(f"Failed to convert last watermark value '{last_watermark_str}' for table {table_name} with type '{watermark_type}'. Performing full load.", exc_info=True)
                      # If conversion fails, disable incremental for this table
                      watermark_column = None
                      last_watermark_value = None
            else:
                 # This block is hit if last_watermark_str is None, '', or 'None'
                 logging.info(f"No valid last watermark found for {table_name} (env var {env_var_name} is '{last_watermark_str}'). Performing full initial load.")
                 watermark_column = None
                 last_watermark_value = None
        else:
             logging.info(f"No watermark column defined or is empty for table {table_name} in tables_list. Performing full load.")
             # Ensure full load query is built
             watermark_column = None
             last_watermark_value = None

        logging.info(f"Final read parameters for {table_name}: watermark_column={watermark_column}, last_watermark_value={last_watermark_value}")


        try:
            logging.info(f"Reading data from source for table: {table_name}")
            # Pass watermark details to the read_table method
            # Note: If watermark_column is None due to checks above, read_table will perform a full load
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
            # The load_table method in PostgresTargetConnector handles insertion.
            target_connector.load_table(table_name, data, column_names)
            logging.info(f"Successfully processed table: {table_name}")

            # --- Calculate and Output New Watermark ---
            if row.get('watermark_column') and str(row.get('watermark_column')).strip() != '' and column_names and data:
                 # Use the original watermark_column name from the tables_list for finding max value
                 original_watermark_column_name = row.get('watermark_column')
                 new_watermark_value = find_max_watermark_value(data, column_names, original_watermark_column_name)

                 if new_watermark_value is not None:
                      # Output the new watermark value in a parsable format for Airflow
                      print(f"NEW_WATERMARK_{table_name.upper()}={new_watermark_value}", flush=True) # Added flush=True
                      logging.info(f"Outputted new watermark for {table_name}: {new_watermark_value}")
                 else:
                      logging.warning(f"Could not determine new watermark for {table_name} despite data being processed.")
            # --- End Calculate and Output New Watermark ---


        except Exception as e:
            logging.error(f"Error processing table {table_name}: {e}", exc_info=True)
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
    except Exception as main_e:
        logging.critical(f"An unhandled error occurred during pipeline execution: {main_e}", exc_info=True)
        sys.exit(1) # Exit with a non-zero code to indicate failure

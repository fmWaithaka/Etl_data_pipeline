import logging
import sys
import argparse
from util import load_db_details, get_tables, create_source_connector, create_target_connector

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
    source_connector = create_source_connector(source_db_config)
    if not source_connector:
        logging.error("Failed to create source connector. Exiting.")
        sys.exit(1)

    target_connector = create_target_connector(target_db_config)
    if not target_connector:
        logging.error("Failed to create target connector. Exiting.")
        sys.exit(1)


    logging.info("Retrieving table list to process...")
    tables_df = get_tables(tables_list_path, table_arg)
    if tables_df is None or tables_df.empty:
        logging.error("No tables found to process based on arguments.")
        sys.exit(1)

    logging.info(f"Tables to process: {tables_df['table_name'].tolist()}")

    # --- Main Processing Loop ---
    for table_name in tables_df["table_name"]:
        logging.info(f"Processing table: {table_name}")

        try:
            logging.info(f"Reading data from source for table: {table_name}")
            # Use the source connector object's method
            data, column_names = source_connector.read_table(table_name)

            if data is None or len(data) == 0:
                logging.warning(f"No data found in source for table: {table_name}. Skipping load.")
                continue

            logging.info(f"Loading data into target for table: {table_name}")
            # Use the target connector object's method
            target_connector.load_table(table_name, data, column_names)
            logging.info(f"Successfully processed table: {table_name}")

        except Exception as e:
            # Catch exceptions during the processing of a single table
            logging.error(f"Error processing table {table_name}: {e}", exc_info=True)
            # Decide whether to continue or exit on error.
            # For ETL, often you want to log the error and continue with the next table.
            # If an error for one table is critical, you might re-raise or sys.exit(1) here.
            # For this example, we'll just log and continue.
            pass

    logging.info("Pipeline processing loop completed.")


if __name__ == "__main__":
    try:
        main()
        logging.info("Pipeline finished successfully.")
    except Exception as main_e:
        # Catch any unhandled exceptions from main
        logging.critical(f"An unhandled error occurred during pipeline execution: {main_e}", exc_info=True)
        sys.exit(1) # Exit with a non-zero code to indicate failure
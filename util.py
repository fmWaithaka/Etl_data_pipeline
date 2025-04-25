import logging
import pandas as pd
from mysql import connector as mc
from mysql.connector import errorcode as ec
import psycopg2
import psycopg2.extras # Import for execute_batch
from typing import Any, Dict, List, Optional, Tuple, Union

# Assuming config.py exists and contains DB_DETAILS
from config import DB_DETAILS

# Get a logger specific to this module
logger = logging.getLogger(__name__)

# --- Raw Connection Helpers (Used internally by connectors) ---
# These functions return raw connection objects. Made internal with leading underscore.

def _get_mysql_connection(db_host: str, db_name: str, db_user: str, db_pass: str) -> Optional[mc.MySQLConnection]:
    """
    Establishes and returns a MySQL database connection.
    Handles basic connection errors.
    """
    try:
        connection = mc.connect(
            user=db_user,
            password=db_pass,
            host=db_host,
            database=db_name
        )
        logger.info("MySQL raw connection successful")
        return connection
    except mc.Error as e:
        if e.errno == ec.ER_ACCESS_DENIED_ERROR:
            logger.error(f"MySQL access denied. Verify credentials for {db_user}@{db_host}", exc_info=True)
        else:
            logger.error(f"MySQL connection error: {str(e)}", exc_info=True)
        return None

def _get_pg_connection(db_host: str, db_name: str, db_user: str, db_pass: str) -> Optional[psycopg2.extensions.connection]:
    """
    Establishes and returns a PostgreSQL database connection.
    Handles basic connection errors.
    """
    try:
        connection = psycopg2.connect(
            dbname=db_name,
            user=db_user,
            host=db_host,
            password=db_pass
        )
        logger.info("PostgreSQL raw connection successful")
        return connection
    except psycopg2.Error as error:
        logger.error(f"PostgreSQL Connection Error: {error}", exc_info=True)
    return None

# --- Base Connector Classes (Optional, but good for defining interface) ---

class SourceConnector:
    """Base class for data source connectors."""
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self._connection: Optional[Any] = None # Connection attribute

    def connect(self) -> Optional[Any]:
        """Establishes the connection."""
        raise NotImplementedError("Subclasses must implement this method")

    def read_table(self, table_name: str) -> Tuple[Optional[List[Tuple]], Optional[List[str]]]:
        """Reads data and column names from a specified table."""
        raise NotImplementedError("Subclasses must implement this method")

    def close(self):
        """Closes the connection."""
        if self._connection:
            try:
                self._connection.close()
                logger.info(f"{self.__class__.__name__} connection closed.")
            except Exception as e:
                 logger.warning(f"Error closing {self.__class__.__name__} connection: {e}")
            self._connection = None


class TargetConnector:
    """Base class for data target connectors."""
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self._connection: Optional[Any] = None # Connection attribute

    def connect(self) -> Optional[Any]:
        """Establishes the connection."""
        raise NotImplementedError("Subclasses must implement this method")

    def load_table(self, table_name: str, data: List[Tuple], column_names: List[str]):
        """Loads data with column names into a specified table."""
        raise NotImplementedError("Subclasses must implement this method")

    def close(self):
        """Closes the connection."""
        if self._connection:
            try:
                self._connection.close()
                logger.info(f"{self.__class__.__name__} connection closed.")
            except Exception as e:
                 logger.warning(f"Error closing {self.__class__.__name__} connection: {e}")
            self._connection = None

# --- Specific Connector Implementations ---

class MySQLSourceConnector(SourceConnector):
    """Source connector for MySQL databases."""
    def connect(self) -> Optional[mc.MySQLConnection]:
        """Establishes a MySQL connection."""
        # Check if connection exists and is still valid before attempting to reconnect
        if self._connection is None or not self._connection.is_connected():
            logger.info("Attempting to connect to MySQL source...")
            self._connection = _get_mysql_connection(
                self.config.get('DB_HOST'),
                self.config.get('DB_NAME'),
                self.config.get('DB_USER'),
                self.config.get('DB_PASS')
            )
            if self._connection:
                 logger.info("MySQL source connection established.")
            else:
                 logger.error("Failed to establish MySQL source connection.")

        return self._connection

    # Integrated logic from read.py
    def read_table(self, table_name: str, limit: int = 0) -> Tuple[Optional[List[Tuple]], Optional[List[str]]]:
        """
        Reads data and column names from a specified MySQL table.
        Uses the connector's internal connection.
        """
        connection = self.connect()
        if not connection:
            logger.error(f"No active MySQL connection available for reading table {table_name}.")
            return None, None

        cursor = None
        try:
            cursor = connection.cursor()
            query = f"SELECT * FROM {table_name}"
            if limit > 0:
                 query += f" LIMIT {limit}"
            logger.info(f"Executing query: {query}")
            cursor.execute(query)

            # Fetch data
            data = cursor.fetchall()

            # Fetch column names from cursor description (MySQL specific)
            # Using cursor.column_names is specific to mysql.connector
            column_names = cursor.column_names if cursor.description else []

            logger.info(f"Successfully read {len(data) if data else 0} rows from {table_name}")
            return data, column_names

        except mc.Error as e:
            logger.error(f"Error reading data from MySQL table {table_name}: {e}", exc_info=True)
            return None, None
        except Exception as e:
             logger.error(f"Unexpected error during MySQL read for table {table_name}: {e}", exc_info=True)
             return None, None
        finally:
            if cursor:
                cursor.close()
            # Connection is managed by the connector instance, not closed here.


class PostgresTargetConnector(TargetConnector):
    """Target connector for PostgreSQL databases."""
    def connect(self) -> Optional[psycopg2.extensions.connection]:
        """Establishes a PostgreSQL connection."""
        # Psycopg2 connections are typically thread/process-safe, but not necessarily robust
        # if the underlying connection is lost. Reconnecting on demand or using
        # connection pooling (more advanced) is common. Simple check here.
        if self._connection is None or self._connection.closed != 0:
             logger.info("Attempting to connect to PostgreSQL target...")
             self._connection = _get_pg_connection(
                 self.config.get('DB_HOST'),
                 self.config.get('DB_NAME'),
                 self.config.get('DB_USER'),
                 self.config.get('DB_PASS')
             )
             if self._connection:
                  logger.info("PostgreSQL target connection established.")
             else:
                  logger.error("Failed to establish PostgreSQL target connection.")

        return self._connection


    def _build_insert_query(self, table_name: str, column_names: List[str]) -> str:
        """Generate an SQL INSERT query for PostgreSQL."""
        # Quote column names to handle potential reserved words or special characters
        column_names_str = ', '.join([f'"{col}"' for col in column_names])
        # Psycopg2 uses %s placeholders
        column_values_str = ', '.join(['%s'] * len(column_names))

        query = f'''
        INSERT INTO "{table_name}" ({column_names_str}) VALUES ({column_values_str});
        '''
        return query

    # Integrated logic from write.py, using psycopg2.extras.execute_batch
    def _insert_data(self, connection: psycopg2.extensions.connection, data: List[Tuple], query: str, batch_size: int = 1000):
        """Insert data into the PostgreSQL database in batches using execute_batch."""
        cursor = None
        try:
            cursor = connection.cursor()
            # psycopg2.extras.execute_batch is more efficient than a manual loop with executemany
            psycopg2.extras.execute_batch(cursor, query, data, page_size=batch_size)
            connection.commit()
            logger.info(f"Inserted {len(data)} rows in batches.")

        except psycopg2.Error as e:
            connection.rollback()
            logger.error(f"Error occurred while inserting data into PostgreSQL: {e}", exc_info=True)
            raise # Re-raise the exception to be caught by the caller
        except Exception as e:
             connection.rollback() # Ensure rollback on any exception during insert
             logger.error(f"Unexpected error during PostgreSQL insert: {e}", exc_info=True)
             raise
        finally:
            if cursor:
                cursor.close()
            # Connection is managed by the connector instance, not closed here.


    # Integrated logic from write.py
    def load_table(self, table_name: str, data: List[Tuple], column_names: List[str]):
        """
        Loads data into a PostgreSQL table using the connector's internal connection.
        """
        connection = self.connect()
        if not connection:
            logger.error(f"No active PostgreSQL connection available for loading table {table_name}.")
            raise ConnectionError(f"Could not connect to PostgreSQL for table {table_name}") # Raise specific error

        try:
            # Build the query
            query = self._build_insert_query(table_name, column_names)

            # Insert the data
            self._insert_data(connection, data, query)
            logger.info(f"Successfully loaded data into PostgreSQL table: {table_name}")

        except Exception as e:
            # Error caught by _insert_data is re-raised, caught here
            logger.error(f"Failed to load data into PostgreSQL table {table_name}.", exc_info=True)
            raise # Re-raise the exception


# --- Connector Factory Functions ---

def create_source_connector(source_db_config: Dict[str, Any]) -> Optional[SourceConnector]:
    """
    Factory function to create the appropriate source connector instance
    based on the source configuration. Attempts to connect upon creation.
    """
    db_type = source_db_config.get('DB_TYPE')
    if db_type == 'mysql':
        connector = MySQLSourceConnector(source_db_config)
        # Attempt connection on creation and return connector only if successful
        if connector.connect():
            return connector
        else:
             # Error logged within connector.connect()
             return None

    # Add other source types here (e.g., 'api', 'file')
    # elif db_type == 'api':
    #     # Assuming you have a connectors package and APISourceConnector class
    #     # from .connectors.api_connector import APISourceConnector
    #     # connector = APISourceConnector(source_db_config)
    #     # if connector.connect():
    #     #      return connector
    #     # else:
    #     #      logger.error("Failed to connect using APISourceConnector.")
    #     #      return None
    #     pass # Placeholder
    else:
        logger.error(f"Unsupported source type specified in configuration: {db_type}")
        return None


def create_target_connector(target_db_config: Dict[str, Any]) -> Optional[TargetConnector]:
    """
    Factory function to create the appropriate target connector instance
    based on the target configuration. Attempts to connect upon creation.
    """
    db_type = target_db_config.get('DB_TYPE')
    if db_type == 'postgres':
        connector = PostgresTargetConnector(target_db_config)
        # Attempt connection on creation and return connector only if successful
        if connector.connect():
             return connector
        else:
             # Error logged within connector.connect()
             return None

    # Add other target types here (e.g., 'file')
    # elif db_type == 'file':
    #     # Assuming you have a connectors package and FileTargetConnector class
    #     # from .connectors.file_connector import FileTargetConnector
    #     # connector = FileTargetConnector(target_db_config)
    #     # if connector.connect():
    #     #      return connector
    #     # else:
    #     #      logger.error("Failed to connect using FileTargetConnector.")
    #     #      return None
    #     pass # Placeholder
    else:
        logger.error(f"Unsupported target type specified in configuration: {db_type}")
        return None

# --- General Utilities ---

def load_db_details(env: str) -> Optional[Dict[str, Dict[str, Any]]]:
    """
    Loads database configuration details for a given environment from config.DB_DETAILS.
    Returns the dictionary containing SOURCE_DB and TARGET_DB config.
    """
    if env in DB_DETAILS:
        logger.info(f"Loaded DB configuration for environment: {env}")
        return DB_DETAILS.get(env) # Use .get() for safety
    else:
        logger.error(f"Environment '{env}' not found in DB_DETAILS configuration.")
        return None


def get_tables(path: str, table_list_arg: str) -> Optional[pd.DataFrame]:
    """
    Reads the tables list from a CSV file and filters based on the table list argument.
    Assumes the CSV has at least 'table_name' and 'to_be_loaded' columns.
    """
    try:
        # Ensure pandas is installed: pip install pandas
        tables_df = pd.read_csv(path, sep=',')
        logger.info(f"Read table list from {path}")

        # Basic validation for required columns
        if 'table_name' not in tables_df.columns or 'to_be_loaded' not in tables_df.columns:
            logger.error(f"Tables list file '{path}' must contain 'table_name' and 'to_be_loaded' columns.")
            return None

        # Filter based on 'to_be_loaded' first
        tables_to_load = tables_df.query('to_be_loaded == "yes"').copy() # Use .copy() to avoid SettingWithCopyWarning

        if table_list_arg.lower() == 'all':
            logger.info("Processing all tables marked 'yes'.")
            return tables_to_load
        else:
            # Filter based on specific table list argument
            requested_tables = [t.strip() for t in table_list_arg.split(',') if t.strip()]
            if not requested_tables:
                 logger.warning("Table list argument provided but was empty after splitting.")
                 return pd.DataFrame(columns=tables_df.columns) # Return empty DataFrame with correct columns

            # Use isin for efficient filtering
            filtered_tables_df = tables_to_load[tables_to_load['table_name'].isin(requested_tables)]

            # Optional: Check if all requested tables were found and marked 'yes'
            found_tables = filtered_tables_df['table_name'].tolist()
            missing_tables = [t for t in requested_tables if t not in found_tables]
            if missing_tables:
                logger.warning(f"Requested tables not found or not marked 'yes' in {path}: {missing_tables}")

            logger.info(f"Processing specified tables marked 'yes': {filtered_tables_df['table_name'].tolist()}")
            return filtered_tables_df

    except FileNotFoundError:
        logger.error(f"Tables list file not found at: {path}", exc_info=True)
        return None
    except pd.errors.EmptyDataError:
        logger.error(f"Tables list file is empty: {path}", exc_info=True)
        return None
    except Exception as e:
        logger.error(f"Error processing tables list file: {e}", exc_info=True)
        return None

# Note: The original get_connection function is now obsolete as connectors manage connections.
# It has been removed.
# Note: The logic for read_table and load_table (including build_insert_query, insert_data)
# has been moved from read.py and write.py into the methods of the connector classes.

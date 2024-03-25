
"""
This script orchestrates the data ingestion process from a JSONL file to the database, 
utilizing OLAP (Online Analytical Processing) approaches to handle large-scale data manipulation 
and analysis. The script employs SQL for logical data manipulation and Python for database interaction, 
without relying on specialized Python libraries like Pandas. Key OLAP practices implemented in 
this process include:

- CTAS (Create Table As Select) for efficient data transformation and loading.
- Staging areas for preliminary data handling and quality checks.
- Data cleansing and deduplication to ensure data integrity.
- Utilization of SQL-based data manipulation for robust and scalable data processing.

These methods are part of a larger OLAP architecture, enabling sophisticated analysis and ensuring 
the data is primed for analytical querying and reporting.
"""

import os
import logging
# from db import BlogAnalysisDB  # Importing db. class
from equalexperts_dataeng_exercise.db import BlogAnalysisDB


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def ingest_data(file_path, db):
    """
    Orchestrates the data ingestion process from a JSONL file to the database.
    """
    try:
        # Create schema 
        db.conn.execute(f"CREATE SCHEMA IF NOT EXISTS blog_analysis;")

        # Step 0: Validate file existence & create db. if not exists
        if not os.path.exists(file_path):
            error_message = f"File {file_path} does not exist."
            logging.error(error_message)
            raise FileNotFoundError(error_message)


        # Step 1: Ingest / load JSON file to staging landing table..
        column_definitions = {
            'Id': 'BIGINT',
            'PostId': 'BIGINT',
            'VoteTypeId': 'BIGINT',
            'CreationDate': 'DATETIME',
            'UserId': 'BIGINT',
            'BountyAmount': 'NUMERIC'
        }
        db.load_json_to_staging_table(file_path=file_path
                                  , column_definitions=column_definitions)
        

        # Step 2: Clean data and set status code for operational loading
        db.cleanse_and_deduplicate_staging_table()

        # Step 3: Move data to operational
        db.move_data_to_operational_with_ctas(column_definitions)


    except Exception as e:
        logging.error(f"An error occurred during the ingestion process: {e}")
        # If you want to propagate the exception up:
        raise


def main():
    # Define file path
    relative_path = '../uncommitted/votes.jsonl'
    file_path = os.path.join(os.path.dirname(__file__), relative_path)

    # Initialize db connection within a context manager to ensure it's properly closed
    try:
        with BlogAnalysisDB() as db:
            ingest_data(file_path, db)
    except Exception as e:
        logging.error(f"An error occurred: {e}")

if __name__ == "__main__":
    main()

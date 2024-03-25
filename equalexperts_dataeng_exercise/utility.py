import os
import logging
# from db import BlogAnalysisDB  # Importing db. class
from equalexperts_dataeng_exercise.db import BlogAnalysisDB


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
debug = True


def ingest_data(file_path, db):
    """
    Orchestrates the data ingestion process from a JSONL file to the database.
    """
    try:
        # Validate file existence
        if not os.path.exists(file_path):
            error_message = f"File {file_path} does not exist."
            logging.error(error_message)
            raise FileNotFoundError(error_message)

       
        logging.info("Setting up staging and operational schemas.")
        table_definitions = {
            "staging_votes": """
                Id VARCHAR,
                PostId VARCHAR,
                VoteTypeId VARCHAR,
                CreationDate VARCHAR,
                UserId VARCHAR,
                BountyAmount VARCHAR,
                DW_INSERT TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            """,
            "votes": """
                Id INTEGER,
                PostId INTEGER,
                VoteTypeId INTEGER,
                CreationDate DATETIME,
                UserId INTEGER,
                BountyAmount NUMERIC(18,2),
                DW_INSERT TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            """
        }

        db.setup_schema("blog_analysis", {"votes": table_definitions["votes"]})

        column_definitions = {
            'Id': 'VARCHAR',
            'PostId': 'VARCHAR',
            'VoteTypeId': 'VARCHAR',
            'CreationDate': 'VARCHAR',
            'UserId': 'VARCHAR',
            'BountyAmount': 'VARCHAR'
        }

        db.load_json_to_staging_table(file_path=file_path
                                  , column_definitions=column_definitions)
        
        # Mimmic archive file movement
        db.fake_file_archive(file_path)

        # Cleansing data and setting status code for operational loading
        db.cleanse_and_deduplicate_staging_table()

        table_mappings = {
            "Id": "Id",
            "PostId": "PostId",
            "VoteTypeId": "VoteTypeId",
            "CreationDate": "CreationDate",
            "UserId": "UserId",
            "BountyAmount": "BountyAmount",
            "DW_INSERT": "CURRENT_TIMESTAMP",
        }

        # Move data to operational
        # db.move_data_to_operational(table_mappings)
        db.move_data_to_operational_with_ctas(column_definitions)

        # TO BE REMOVED
        if debug:
            
            print(1)

            
            # Fetch and print the total record count from stage table
            total_records = db.conn.execute("SELECT COUNT(*) FROM blog_analysis.staging_votes_load;").fetchone()[0]
            print(f"Total records in stage load: {total_records}")
            total_records = db.conn.execute("SELECT COUNT(*) FROM blog_analysis.staging_votes;").fetchone()[0]
            print(f"Total records in stage: {total_records}")

            # Fetch and print the total record count from votes table
            total_records_votes = db.conn.execute("SELECT COUNT(*) FROM blog_analysis.votes;").fetchone()[0]
            print(f"Total records in votes: {total_records_votes}")

            numb_of_record = 50
            # # Fetch and print the first n rows from stage table
            # first_n_staging = db.conn.execute(f"SELECT * FROM blog_analysis.staging_votes_load ORDER BY staging_status LIMIT {numb_of_record};").fetchall()
            # print("records from staging_votes_load:")
            # for record in first_n_staging:
            #     print(record)

            # Fetch and print the first n rows from votes table
            first_n_staging = db.conn.execute(f"SELECT * FROM blog_analysis.staging_votes LIMIT {numb_of_record};").fetchall()
            print("records from staging_votes:")
            for record in first_n_staging:
                print(record)

            # # Fetch and print the first n rows from votes table
            # first_n_staging = db.conn.execute(f"SELECT * FROM blog_analysis.votes LIMIT {numb_of_record};").fetchall()
            # print("records from votes:")
            # for record in first_n_staging:
            #     print(record)

            # for table in ['staging_votes_load','staging_votes']:
            #     db.get_table_schema_info(f'{table}')

            # db.conn.execute(f"DROP TABLE blog_analysis.votes;").fetchall()


    except Exception as e:
        logging.error(f"An error occurred during the ingestion process: {e}")
        # If you want to propagate the exception up:
        raise


def main():
    # Define file path
    relative_path = '../uncommitted/sample-votes-dups.jsonl'
    file_path = os.path.join(os.path.dirname(__file__), relative_path)

    # Initialize db connection within a context manager to ensure it's properly closed
    try:
        with BlogAnalysisDB() as db:
            ingest_data(file_path, db)
    except Exception as e:
        logging.error(f"An error occurred: {e}")

if __name__ == "__main__":
    main()



from datetime import datetime

def cleanse_and_deduplicate_row(id, post_id, vote_type_id, creation_date, user_id, bounty_amount, row_number):
    # Initialize error messages list
    errors = []

    # Check for null values and data type validations
    try:
        _id = int(id) if id is not None else None
    except ValueError:
        errors.append('Invalid Id')

    try:
        _creation_date = datetime.strptime(creation_date, '%Y-%m-%dT%H:%M:%S.%f') if creation_date else None
    except ValueError:
        errors.append('Invalid CreationDate')

    if row_number > 1:
        errors.append('Duplicate record')

    # Determine the status based on the presence of errors
    status = 'FAILED' if errors else 'READYTOLOAD'

    # Construct the error description
    error_description = '; '.join(errors) if errors else None

    return status, error_description



# register it 
import duckdb

# Create a connection to DuckDB
con = duckdb.connect()

# Register the UDF
con.create_function("cleanse_and_deduplicate_row", cleanse_and_deduplicate_row,
                    parameters=["VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "BIGINT"],
                    return_type="STRUCT<status VARCHAR, error_description VARCHAR>")



result = con.execute("""
SELECT cleanse_and_deduplicate_row(Id, PostId, VoteTypeId, CreationDate, UserId, BountyAmount, rn).* 
FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY Id ORDER BY CreationDate DESC) AS rn FROM staging_votes_load) t
""").fetchall()

# Print or process the result
print(result)




# db.py

import duckdb
from datetime import datetime

class BlogAnalysisDB:
    def __init__(self, db_path=':memory:'):
        self.conn = duckdb.connect(db_path)
        self.register_udfs()

    def register_udfs(self):
        # Define the UDF
        def cleanse_and_deduplicate_row(id, post_id, vote_type_id, creation_date, user_id, bounty_amount, row_number):
            # UDF logic as previously defined
            pass

        # Register the UDF with DuckDB
        self.conn.create_function("cleanse_and_deduplicate_row", cleanse_and_deduplicate_row,
                                  parameters=["VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "BIGINT"],
                                  return_type="STRUCT<status VARCHAR, error_description VARCHAR>")

    # Other methods...

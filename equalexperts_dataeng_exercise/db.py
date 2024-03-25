import duckdb
import logging

class BlogAnalysisDB:
    def __init__(self, db_path='warehouse.db'):
        self.db_path = db_path
        self._connect()

    def _connect(self):
        try:
            self.conn = duckdb.connect(self.db_path)
            logging.info("Connected to DuckDB successfully.")
        except Exception as e:
            logging.error("Failed to connect to DuckDB: %s", e)
            self.conn = None


    def test_connection(self):
        """Test the database connection by executing a simple query."""
        try:
            self.conn.execute("SELECT 1").fetchall()
            return True
        except Exception as e:
            logging.error("Database connection test failed: %s", e)
            return False


    def __enter__(self):
        self._connect()
        return self


    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


    def close(self):
        if self.conn:
            self.conn.close()
            logging.info("Database connection closed.")


    def setup_schema(self, table_name, column_definitions):
        schema_name = "blog_analysis"
        try:
            self.conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
            # Construct the column definitions into SQL format
            columns_sql = ', '.join([f"{col_name} {data_type}" for col_name, data_type in column_definitions.items()])
            # Create the table with the defined columns
            self.conn.execute(f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} ({columns_sql});")
            logging.info(f"Table {table_name} in schema {schema_name} is set up successfully with columns.")
        except Exception as e:
            logging.error(f"Error setting up table {table_name} in schema {schema_name}: {e}")
            raise e


    def get_table_schema_info(self, table_name):
        try:
            # Define the SQL query to fetch table structure information
            query = f"""
            SELECT
                col.table_schema,
                col.table_name,
                col.column_name,
                col.data_type,
                col.is_nullable,
                col.ordinal_position
            FROM
                information_schema.columns AS col
            WHERE
                col.table_name = '{table_name}';
            """

            result_set = self.conn.execute(query).fetchall()

            # Print the results
            for row in result_set:
                print(f"Schema: {row[0]}, Table: {row[1]}, Column: {row[2]}, Type: {row[3]}, Nullable: {row[4]}, Position: {row[5]}")
        
        except Exception as e:
            # Handle the exception
            logging(f"An error occurred: {e}")
            raise e


    def load_json_to_staging_table(self, file_path, column_definitions):
        """
        Creates a new table from a JSON file using DuckDB's read_json function.
        
        Args:
            file_path (str): The path to the JSON file.
            column_definitions (dict): The columns and their SQL types.
        """
        table_name = "blog_analysis.staging_votes_load"
        try:
            # Construct the columns part of the SQL query from the column_definitions
            columns_sql = ', '.join([f"'{col}': 'VARCHAR'" for col, dtype in column_definitions.items()])

            # Construct the CREATE TABLE AS SELECT query
            create_table_query = f"""
                CREATE OR REPLACE TABLE {table_name} AS
                SELECT *
                FROM read_json('{file_path}',
                                format = 'newline_delimited',
                                ignore_errors = true,
                                columns = {{{columns_sql}}});
            """
            # Execute the query
            self.conn.execute(create_table_query)
            logging.info(f"Table {table_name} created successfully from {file_path}.")
        
        except Exception as e:
            # Log the error if table creation fails
            logging.error(f"Error creating table {table_name} from {file_path}: {e}")
            raise e


    def cleanse_and_deduplicate_staging_table(self):
        """
        Cleanses the data in the staging_votes table, checks for duplicates, and creates 
        a new table with status and error descriptions.
        """
        table_name_load = "blog_analysis.staging_votes_load"
        table_name = "blog_analysis.staging_votes"
        try:
            # Define the SQL command for data cleansing and deduplication
            cleanse_dedupe_query = f"""
                CREATE OR REPLACE TABLE {table_name} AS
                SELECT 
                    *,
                    CASE 
                        WHEN Id IS NULL OR try_cast(Id as BIGINT) IS NULL THEN 'FAILED'
                        WHEN CreationDate IS NULL OR try_cast(CreationDate as TIMESTAMP) IS NULL THEN 'FAILED'
                        WHEN PostId IS NOT NULL AND try_cast(PostId as BIGINT) IS NULL THEN 'FAILED'
                        WHEN VoteTypeId IS NOT NULL AND try_cast(VoteTypeId as BIGINT) IS NULL THEN 'FAILED'
                        WHEN UserId IS NOT NULL AND try_cast(UserId as BIGINT) IS NULL THEN 'FAILED'
                        WHEN BountyAmount IS NOT NULL AND try_cast(BountyAmount as DECIMAL(18,2)) IS NULL THEN 'FAILED'
                        WHEN rn > 1 THEN 'DUPLICATE'
                        ELSE 'READYTOLOAD'
                    END AS staging_status,
                    CONCAT_WS('; ',
                        CASE WHEN Id IS NULL OR try_cast(Id as BIGINT) IS NULL THEN 'Invalid Id' ELSE NULL END,
                        CASE WHEN CreationDate IS NULL OR try_cast(CreationDate as TIMESTAMP) IS NULL THEN 'Invalid CreationDate' ELSE NULL END,
                        CASE WHEN PostId IS NOT NULL AND try_cast(PostId as BIGINT) IS NULL THEN 'Invalid PostId' ELSE NULL END,
                        CASE WHEN VoteTypeId IS NOT NULL AND try_cast(VoteTypeId as BIGINT) IS NULL THEN 'Invalid VoteTypeId' ELSE NULL END,
                        CASE WHEN UserId IS NOT NULL AND try_cast(UserId as BIGINT) IS NULL THEN 'Invalid UserId' ELSE NULL END,
                        CASE WHEN BountyAmount IS NOT NULL AND try_cast(BountyAmount as DECIMAL(18,2)) IS NULL THEN 'Invalid BountyAmount' ELSE NULL END,
                        CASE WHEN rn > 1 THEN 'Duplicate record' ELSE NULL END
                    ) AS error_description
                FROM (
                    SELECT 
                        *,
                        ROW_NUMBER() OVER (PARTITION BY Id ORDER BY CreationDate DESC) as rn
                    FROM {table_name_load}
                ) AS ranked;
            """

            # Execute the data cleansing and deduplication command
            self.conn.execute(cleanse_dedupe_query)

            logging.info(f"Staging table successfully cleansed, duplicates checked, and new table {table_name} created.")

        except Exception as e:
            logging.error(f"Error during data cleansing and deduplication: {e}")
            raise e


    def move_data_to_operational(self, table_mappings):
        """
        Deletes existing records in the operational table and moves unique data from
        the staging area to the operational area based on the most recent CreationDate,
        ensuring that only the latest record for each unique 'Id' is kept.
        Args:
            table_mappings (dict): A dictionary mapping operational table columns to staging table columns.
        """
        try:
            # Step 1: Deleting matching records in the operational table
            delete_query = """
                DELETE FROM blog_analysis.votes
                WHERE EXISTS (
                    SELECT 1 
                    FROM blog_analysis.staging_votes 
                    WHERE blog_analysis.staging_votes.Id = blog_analysis.votes.Id
                    AND staging_status = 'READYTOLOAD'

                );
            """
            self.conn.execute(delete_query)
            logging.info("Existing matching records deleted from the operational table.")

            # Step 2: Construct the dynamic column part of the SQL query from the table_mappings
            operational_columns = ', '.join(table_mappings.keys())
            staging_columns = ', '.join(table_mappings.values())

            # Step 3: Inserting records from staging into operational
            insert_query = f"""
                INSERT INTO blog_analysis.votes ({operational_columns})
                SELECT {staging_columns}
                FROM (
                    SELECT *
                    FROM blog_analysis.staging_votes
                    WHERE staging_status = 'READYTOLOAD'
                ) sub;
            """

            self.conn.execute(insert_query)
            logging.info("Unique records successfully moved from staging to operational.")

            # Final step 4: Create indexes on specific columns in the operational table
            index_columns = ['Id', 'CreationDate']  # Specify only the columns you need indexed
            for column_name in index_columns:
                try:
                    # In SQL databases supporting index drop
                    self.conn.execute(f"DROP INDEX IF EXISTS {column_name}_idx;")
                except Exception as e:
                    logging.info(f"Could not drop index {column_name}_idx. Reason: {e}")

                self.conn.execute(f"CREATE INDEX IF NOT EXISTS {column_name}_idx ON blog_analysis.votes ({column_name});")


        except Exception as e:
            logging.error(f"Error moving data from staging to operational: {e}")
            raise e


    def move_data_to_operational_with_ctas(self, column_mappings):
        """
        Merges existing records in the operational table with new and updated records
        from the staging table using a CTAS approach. Also adds indexes to the new table.

        Args:
            column_mappings (dict): Mapping of column names from staging to operational table
            with data types.
        """
        try:
            # Step 0: Ensure the operational table exists
            self.setup_schema("votes",column_mappings)

            # Step 1: Construct column SQL for SELECT clause based on column mappings
            select_sql_schema = ', '.join([f"{src_col}" for src_col in column_mappings.keys()])
            select_sql_operational = ', '.join([f"operational.{dest_col}" for dest_col in column_mappings.keys()])
            select_sql_schema_cast = ', '.join([f"cast({col_name} AS {data_type}) AS {col_name}" 
                                                for col_name, data_type in column_mappings.items()])
            
            # Step 2: Combined dataset of unique latest records from staging and unmatched records from operational
            combined_table_query = f"""
                CREATE OR REPLACE TABLE combined_votes AS
                SELECT {select_sql_schema}
                FROM blog_analysis.staging_votes
                WHERE staging_status = 'READYTOLOAD'

                UNION ALL
                
                SELECT {select_sql_operational}
                FROM blog_analysis.votes operational
                LEFT JOIN blog_analysis.staging_votes staging ON operational.Id = staging.Id
                    AND staging_status = 'READYTOLOAD' -- make sure only clean data makes it to the comparison. 
                WHERE staging.Id IS NULL ;
            """
            self.conn.execute(combined_table_query)

            # Step 3: Recreate the operational table with the combined dataset
            recreate_operational_table = f"""
                CREATE OR REPLACE TABLE blog_analysis.votes AS
                SELECT {select_sql_schema_cast} FROM combined_votes;
            """
            self.conn.execute(recreate_operational_table)

            # Final step 4: Create indexes on specific columns in the operational table
            index_columns = ['Id', 'CreationDate']  # Specify only the columns you need indexed
            for column_name in index_columns:
                try:
                    # In SQL databases supporting index drop
                    self.conn.execute(f"DROP INDEX IF EXISTS {column_name}_idx;")
                except Exception as e:
                    logging.info(f"Could not drop index {column_name}_idx. Reason: {e}")

                self.conn.execute(f"CREATE INDEX IF NOT EXISTS {column_name}_idx ON blog_analysis.votes ({column_name});")

            logging.info("Operational table successfully updated with combined data and indexes using CTAS approach.")

        except Exception as e:
            logging.error(f"Error merging, updating, and indexing operational table with CTAS: {e}")
            raise e


    def create_outlier_weeks_view(self):
        """Create or replace the 'outlier_weeks' view in the database."""
        try:
            query = """
                CREATE OR REPLACE VIEW blog_analysis.outlier_weeks AS
                WITH WeeklyVotes AS (
                    SELECT
                        EXTRACT(YEAR FROM CreationDate) AS Year,
                        CASE 
                            WHEN EXTRACT(MONTH FROM CreationDate) = 1 
                                    AND EXTRACT(DAY FROM CreationDate) <= 7 
                                    AND EXTRACT(ISODOW FROM CreationDate) >= 4 
                                THEN 0 -- Assign 'week 0' for days in the first week of January that would traditionally be part of the last week of the previous year according to ISO standards
                            ELSE
                                EXTRACT(WEEK FROM CreationDate) -- Use standard week number for other cases
                        END AS CustomWeekNumber,
                        COUNT(*) AS VoteCount
                    FROM blog_analysis.votes
                    GROUP BY Year, CustomWeekNumber
                ), AvgVotes AS (
                    SELECT AVG(VoteCount) AS AvgVoteCount
                    FROM WeeklyVotes
                )
                SELECT 
                    w.Year, 
                    w.CustomWeekNumber AS WeekNumber, 
                    w.VoteCount
                FROM 
                    WeeklyVotes w, AvgVotes av
                WHERE
                    w.VoteCount < 0.8 * av.AvgVoteCount OR
                    w.VoteCount > 1.2 * av.AvgVoteCount
                ORDER BY 
                    w.Year, 
                    w.CustomWeekNumber;
            """
            self.conn.execute(query)
            logging.info("Outlier weeks view created successfully.")

        except Exception as e:
            logging.error(f"Error replacing Outlier weeks view: {e}")
            raise e










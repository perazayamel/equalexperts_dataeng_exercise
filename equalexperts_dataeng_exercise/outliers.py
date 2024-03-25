import logging
# from db import BlogAnalysisDB  # Importing db. class
from equalexperts_dataeng_exercise.db import BlogAnalysisDB


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def calculate_outliers(db):
    """
    Orchestrates the outlier calculation process from a duckbd table to a view in the duckdb database.
    """
    try:
        # Computes custom week # 0 and outliers view
        db.create_outlier_weeks_view()

    except Exception as e:
        logging.error(f"An error occurred during the ingestion process: {e}")
        # If you want to propagate the exception up:
        raise


def main():
    # Initialize db connection within a context manager to ensure it's properly closed
    try:
        with BlogAnalysisDB() as db:
            calculate_outliers(db)
    except Exception as e:
        logging.error(f"An error occurred: {e}")

if __name__ == "__main__":
    main()

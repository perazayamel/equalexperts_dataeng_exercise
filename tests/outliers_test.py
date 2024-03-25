import os
import sys
from pathlib import Path

# This adds the project directory to the Python path to resolve the `db` import
sys.path.append(str(Path(__file__).resolve().parents[1]))

import pytest
from equalexperts_dataeng_exercise.db import BlogAnalysisDB
from equalexperts_dataeng_exercise.ingest import ingest_data
from equalexperts_dataeng_exercise.outliers import calculate_outliers

"""
NOTE:

If you encounter issues while attempting to run this test from the project root using the command:
    pytest tests/outliers_test.py

Please navigate to the 'outlier.py' file and adjust the import statement for BlogAnalysisDB as follows:

Change from:
    from db import BlogAnalysisDB
to:
    from equalexperts_dataeng_exercise.db import BlogAnalysisDB

This adjustment aligns the import path with the project structure and should resolve import-related errors when running the test.

To execute the test implementation from the project terminal, use the command:
    pytest tests/outliers_test.py

IMPORTANT WARNING:

After completing the testing, it is crucial to revert the changes made to the 'outliers.py' file. 
Failure to revert these changes might lead to unexpected behavior when running the application in production or other environments.

Please ensure the following line in 'outliers.py' is changed back to:
    from db import BlogAnalysisDB

This ensures that your application maintains the correct import paths outside of the testing environment.

"""

@pytest.fixture(params=[
    # ('samples-votes-outliers1.jsonl', 10, [
    #     {"Year": 2023, "WeekNumber": 0, "VoteCount": 4},
    #     {"Year": 2023, "WeekNumber": 1, "VoteCount": 1}
    # ]),
    # ('samples-votes-outliers2.jsonl', 10, [
    #     {"Year": 2022, "WeekNumber": 52, "VoteCount": 4},
    #     {"Year": 2023, "WeekNumber": 0, "VoteCount": 3}
    # ]),
    # ('samples-votes-outliers3.jsonl', 10, [
    #     {"Year": 2022, "WeekNumber": 1, "VoteCount": 1},
    #     {"Year": 2022, "WeekNumber": 2, "VoteCount": 3},
    #     {"Year": 2022, "WeekNumber": 11, "VoteCount": 1},
    #     {"Year": 2023, "WeekNumber": 1, "VoteCount": 1}
    # ]),
    ('samples-votes-outliers4.jsonl', 5, [
    ]),
    ('samples-votes.jsonl', 16, [
        {"Year": 2022, "WeekNumber": 0, "VoteCount": 1},
        {"Year": 2022, "WeekNumber": 1, "VoteCount": 3},
        {"Year": 2022, "WeekNumber": 2, "VoteCount": 3},
        {"Year": 2022, "WeekNumber": 5, "VoteCount": 1},
        {"Year": 2022, "WeekNumber": 6, "VoteCount": 1},
        {"Year": 2022, "WeekNumber": 8, "VoteCount": 1}
    ]),
])
def file_info(request):
    file_name, expected_count, expected_outliers = request.param
    relative_path = f'../uncommitted/{file_name}'
    file_path = os.path.join(os.path.dirname(__file__), relative_path)
    return file_path, expected_count, expected_outliers

@pytest.fixture
def db():
    # Setup a test database instance
    db_instance = BlogAnalysisDB(db_path=':memory:')
    yield db_instance  # Yield the database instance for testing
    db_instance.close()  # Cleanup after tests


def test_ingest_process(db, file_info):
    test_file_path, expected_count, _ = file_info
    assert os.path.exists(test_file_path), "Test JSONL file must exist"
    ingest_data(test_file_path, db)
    staging_results = db.conn.execute("SELECT COUNT(*) FROM blog_analysis.staging_votes").fetchone()[0]
    operational_results = db.conn.execute("SELECT COUNT(*) FROM blog_analysis.votes").fetchone()[0]
    assert staging_results > 0, "Staging table should contain data"
    assert operational_results == expected_count, f"Expected {expected_count} valid records in operational table, got {operational_results}"


def test_outlier_view_contents(db, file_info):
    test_file_path, _, expected_outliers = file_info
    assert os.path.exists(test_file_path), "Test JSONL file must exist"
    # Call the ingest function to ensure the necessary database objects and data are present for the test.
    ingest_data(test_file_path, db)  

    # Create the outlier_weeks view
    calculate_outliers(db)

    # Query the outlier_weeks view
    fetched_results = db.conn.execute("SELECT Year, WeekNumber, VoteCount FROM blog_analysis.outlier_weeks ORDER BY Year, WeekNumber").fetchall()
    
    # Transform fetched results to a list of dictionaries for easier comparison
    fetched_results_dicts = [{"Year": year, "WeekNumber": week, "VoteCount": votes} for year, week, votes in fetched_results]

    # Assert that the fetched results match the expected outcomes
    assert fetched_results_dicts == expected_outliers, "The fetched results do not match the expected outlier weeks."

def test_view_idempotency(db, file_info):
    # Ingest data and create the outlier_weeks view
    test_file_path, _, _ = file_info
    # Call the ingest function to ensure the necessary database objects and data are present for the test.
    ingest_data(test_file_path, db)  

    calculate_outliers(db)

    # Fetch and store the initial view contents
    initial_results = db.conn.execute(
        "SELECT Year, WeekNumber, VoteCount FROM blog_analysis.outlier_weeks ORDER BY Year, WeekNumber"
    ).fetchall()

    # Recreate the outlier_weeks view
    calculate_outliers(db)

    # Fetch and store the recreated view contents
    recreated_results = db.conn.execute(
        "SELECT Year, WeekNumber, VoteCount FROM blog_analysis.outlier_weeks ORDER BY Year, WeekNumber"
    ).fetchall()

    # Compare the initial and recreated view contents
    assert initial_results == recreated_results, "The view contents should remain constant after recreation, indicating idempotency."

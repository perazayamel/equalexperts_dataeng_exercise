import os
import sys
from pathlib import Path

# This adds the project directory to the Python path to resolve the `db` import
sys.path.append(str(Path(__file__).resolve().parents[1]))

import pytest
from equalexperts_dataeng_exercise.db import BlogAnalysisDB
from equalexperts_dataeng_exercise.ingest import ingest_data

"""
NOTE:

If you encounter issues while attempting to run this test from the project root using the command:
    pytest tests/ingest_test.py

Please navigate to the 'ingest.py' file and adjust the import statement for BlogAnalysisDB as follows:

Change from:
    from db import BlogAnalysisDB
to:
    from equalexperts_dataeng_exercise.db import BlogAnalysisDB

This adjustment aligns the import path with the project structure and should resolve import-related errors when running the test.

To execute the test implementation from the project terminal, use the command:
    pytest tests/ingest_test.py

IMPORTANT WARNING:

After completing the testing, it is crucial to revert the changes made to the 'ingest.py' file. 
Failure to revert these changes might lead to unexpected behavior when running the application in production or other environments.

Please ensure the following line in 'ingest.py' is changed back to:
    from db import BlogAnalysisDB

This ensures that your application maintains the correct import paths outside of the testing environment.

"""


# Parameterized fixture to handle different test files and expected counts
@pytest.fixture(params=[
    ('samples-votes.jsonl', 16), # OK
    ('sample-votes-dups.jsonl', 8), # OK
    ('sample-votes-invalid-datatypes.jsonl', 13), # OK
    ('sample-votes-invalid-CreationDates.jsonl',13), # OK
    ('sample-votes-invalid-Id.jsonl',10), # OK
    ('votes.jsonl',40299), # OK
    ('sample-votes-PostId.jsonl',5), # OK
    ('sample-votes-VoteTypeId.jsonl',5), # OK
])
def file_info(request):
    file_name, expected_count = request.param
    relative_path = f'../uncommitted/{file_name}'
    file_path = os.path.join(os.path.dirname(__file__), relative_path)
    return file_path, expected_count


@pytest.fixture
def db():
    # Setup a test database instance
    db_instance = BlogAnalysisDB(db_path=':memory:')
    yield db_instance  # Yield the database instance for testing
    db_instance.close()  # Cleanup after tests


def test_ingest_missing_file_raises_error(db):
    # Define a non-existent file path
    file_path = f'../uncommitted/nofile.jsonl'
    non_existent_file_path = os.path.join(os.path.dirname(__file__), file_path)
    
    # Attempt to ingest data from a non-existent file and expect a FileNotFoundError
    with pytest.raises(FileNotFoundError):
        ingest_data(non_existent_file_path, db)


# Tests for the ingestion process using the parameterized file_info
def test_ingest_process(db, file_info):
    test_file_path, _ = file_info
    assert os.path.exists(test_file_path), "Test JSONL file must exist"
    ingest_data(test_file_path, db)
    staging_results = db.conn.execute("SELECT COUNT(*) FROM blog_analysis.staging_votes").fetchone()[0]
    operational_results = db.conn.execute("SELECT COUNT(*) FROM blog_analysis.votes").fetchone()[0]
    assert staging_results > 0, "Staging table should contain data"
    assert operational_results > 0, "Operational table should contain data"


def test_invalid_data_types(db, file_info):
    test_file_path, expected_count = file_info
    ingest_data(test_file_path, db)
    valid_records_count = db.conn.execute("SELECT COUNT(*) FROM blog_analysis.votes").fetchone()[0]
    assert valid_records_count == expected_count, f"Expected {expected_count} valid records, got {valid_records_count}"


def test_handling_duplicates(db, file_info):
    test_file_path, _ = file_info
    ingest_data(test_file_path, db)
    unique_records_count = db.conn.execute("SELECT COUNT(DISTINCT Id) FROM blog_analysis.votes").fetchone()[0]
    total_records_count = db.conn.execute("SELECT COUNT(*) FROM blog_analysis.votes").fetchone()[0]
    assert unique_records_count == total_records_count, "No duplicates should exist in the operational table"


def test_idempotent_data_loading(db, file_info):
    test_file_path, expected_count = file_info
    for _ in range(3):  # Load the same JSON file multiple times
        ingest_data(test_file_path, db)
    records_count_after_loads = db.conn.execute("SELECT COUNT(*) FROM blog_analysis.votes").fetchone()[0]
    assert records_count_after_loads == expected_count, f"Record count should remain constant at {expected_count}, got {records_count_after_loads}"


@pytest.mark.parametrize("file_info_set", [
    ([('samples-votes-incremental.jsonl', 14), 
      ('samples-votes.jsonl', 16)], 30),
])
def test_incremental_ingestion(db, file_info_set):
    """
    Test that new records from different files are correctly added to the operational table.
    """
    file_set, expected_total = file_info_set

    total_records = 0
    for file_info in file_set:
        file_name, expected_count = file_info
        relative_path = f'../uncommitted/{file_name}'
        file_path = os.path.join(os.path.dirname(__file__), relative_path)
        assert os.path.exists(file_path), f"Test JSONL file {file_name} must exist"

        ingest_data(file_path, db)
        new_records_count = db.conn.execute("SELECT COUNT(*) FROM blog_analysis.votes").fetchone()[0]
        total_records += expected_count
        assert new_records_count == total_records, f"Expected {total_records} records after ingesting {file_name}, but found {new_records_count}"

    # Check if the total number of records in the operational table matches the expected total
    final_records_count = db.conn.execute("SELECT COUNT(*) FROM blog_analysis.votes").fetchone()[0]
    assert final_records_count == expected_total, f"Total expected records mismatch: expected {expected_total}, got {final_records_count}"


def test_upsert_records_added(db):
    # Ingest data from the first file
    file_path = f'../uncommitted/samples-votes.jsonl'
    file1_path = os.path.join(os.path.dirname(__file__), file_path)
    
    ingest_data(file1_path, db)
    
    # Count records after ingesting file1
    records_count_after_file1 = db.conn.execute("SELECT COUNT(DISTINCT Id) FROM blog_analysis.votes").fetchone()[0]

    # Ingest data from the second file
    file_path = f'../uncommitted/samples-votes-upsert.jsonl'
    file2_path = os.path.join(os.path.dirname(__file__), file_path)
 
    ingest_data(file2_path, db)
    
    # Count records after ingesting file2
    records_count_after_file2 = db.conn.execute("SELECT COUNT(DISTINCT Id) FROM blog_analysis.votes").fetchone()[0]
    
    # Assert that the total records are the sum of unique records in file1 and new records from file2
    # This assumes file2 has new records that are not in file1, change numbers as per your exact file content
    assert records_count_after_file1 == 16, f"Expected 16 unique records after ingesting file1, got {records_count_after_file1}"
    assert records_count_after_file2 == 24, f"Expected 24 unique records after ingesting file2, got {records_count_after_file2}"


@pytest.mark.parametrize("file_info_set, expected_total_records, expected_VoteTypeId", [
    ([('samples-votes-upsert.jsonl', 16, '2'), 
      ('samples-votes-upsert-col.jsonl', 16, 100)], 16, 100),
])
def test_upsert_column(db, file_info_set, expected_total_records, expected_VoteTypeId):
    # Your test code follows here

    # Iterate through each file and its expected record count
    for file_info in file_info_set:
        file_path, _, _ = file_info

        file_path = f'../uncommitted/{file_path}'
        full_file_path = os.path.join(os.path.dirname(__file__), file_path)
        
        # Ingest the file
        ingest_data(full_file_path, db)

    # Assert the total number of unique records in the operational table
    total_records_count = db.conn.execute("SELECT COUNT(*) FROM blog_analysis.votes").fetchone()[0]
    assert total_records_count == expected_total_records, f"Total record count mismatch. Expected: {expected_total_records}, Found: {total_records_count}"
    VoteTypeId = db.conn.execute("SELECT VoteTypeId FROM blog_analysis.votes WHERE Id = 1;").fetchone()[0]
    assert VoteTypeId == expected_VoteTypeId, f"VoteTypeId date value mismatch. Expected: {expected_VoteTypeId}, Found: {VoteTypeId}"

    

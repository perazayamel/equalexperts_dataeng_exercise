import sys
from pathlib import Path

# This adds the project directory to the Python path to resolve the `db` import
sys.path.append(str(Path(__file__).resolve().parents[1]))

import pytest
from equalexperts_dataeng_exercise.db import BlogAnalysisDB

# Command to test db.py from project terminal => pytest tests/db_test.py


# Define table definitions for testing
STAGING_VOTES_DEFINITIONS = {
    "Id": "VARCHAR(225)",
    "PostId": "VARCHAR(225)",
    "VoteTypeId": "VARCHAR(225)",
    "CreationDate": "VARCHAR(225)",
    "UserId": "VARCHAR(225)",
    "BountyAmount": "VARCHAR(225)"
}

VOTES_DEFINITIONS = {
    "Id": "BIGINT",
    "PostId": "BIGINT",
    "VoteTypeId": "BIGINT",
    "CreationDate": "DATETIME",
    "UserId": "BIGINT",
    "BountyAmount": "NUMERIC(18,2)"
}


@pytest.fixture
def db():
    # Use an in-memory database for tests
    db_instance = BlogAnalysisDB(db_path=':memory:')
    # Set up the schema and tables for tests
    db_instance.setup_schema("staging_votes", STAGING_VOTES_DEFINITIONS)
    db_instance.setup_schema("votes", VOTES_DEFINITIONS)
    yield db_instance  # Yield the database instance for testing
    db_instance.close()  # Ensure cleanup


def test_database_connection(db):
    # Test that the database connection works
    assert db.test_connection() is True, "Database connection should be successful"

TEST_TABLES = ["staging_votes", "votes"]
def test_schema_setup(db):
    # Test that the schema and tables were set up correctly
    for table in TEST_TABLES:
        result = db.conn.execute(f"""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_schema = 'blog_analysis' AND table_name = '{table}';
        """).fetchall()
        assert result[0][0] == 1, f"Table {table} should exist in schema blog_analysis"


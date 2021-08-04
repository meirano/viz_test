import hashlib
import psycopg2
import os

from psycopg2.extras import DictCursor
from typing import List, NamedTuple

SOURCE_HOST = "queenie.db.elephantsql.com"
SOURCE_USER = "zxknztkn"
SOURCE_PASSWORD_VARIABLE = "POSTGRESS_SOURCE_PASSWORD"
DEST_HOST = "queenie.db.elephantsql.com"
DEST_USER = "ccnwfpvr"
DEST_PASSWORD_VARIABLE = "POSTGRESS_DEST_PASSWORD"


class TableInfo(NamedTuple):
    query: str
    fields_to_hash: List[str]


CREATE_SCANS_AND_RESULTS_TABLE = """
    CREATE TABLE IF NOT EXISTS scans_and_results (
	study_uid VARCHAR(256),
	viz_lvo BOOL,
	first_acquired VARCHAR(256),
	patient_first_acquired VARCHAR(256),
	patient_institution VARCHAR(256),
	patient VARCHAR(256)
)
"""

CREATE_USERS_TABLE = """
    CREATE TABLE IF NOT EXISTS users (
	name VARCHAR(256),
	address VARCHAR(256),
	user_uid VARCHAR(256)
)
"""

CREATE_NOTIFICATIONS_TABLE = """
    CREATE TABLE IF NOT EXISTS notifications (
	study_uid VARCHAR(256),
	notification_time TIMESTAMP,
	patient VARCHAR(256),
	users varchar(350)
)
"""

TABLES = {
    "scans_and_results": TableInfo(
        query=CREATE_SCANS_AND_RESULTS_TABLE,
        fields_to_hash=[
            "study_uid",
            "first_acquired",
            "patient_first_acquired",
            "patient_first_acquired",
        ],
    ),
    "users": TableInfo(query=CREATE_USERS_TABLE, fields_to_hash=["name", "address", "user_uid"]),
    "notifications": TableInfo(
        query=CREATE_NOTIFICATIONS_TABLE, fields_to_hash=["study_uid", "patient", "users"]
    ),
}


def create_tables(conn):
    with conn.cursor() as cursor:
        for table_info in TABLES.values():
            cursor.execute(table_info.query)


def insert_statement_from_dict(dict, table):
    return f"""
        INSERT INTO {table} ({",".join([key for key in dict.keys()])})
        VALUES ('{"','".join([str(value) for value in dict.values()])}')
    """


def pipeline():
    source_conn = psycopg2.connect(
        host=SOURCE_HOST,
        port=5432,
        database=SOURCE_USER,
        user=SOURCE_USER,
        password=os.environ[SOURCE_PASSWORD_VARIABLE],
    )
    dest_conn = psycopg2.connect(
        host=DEST_HOST,
        port=5432,
        database=DEST_USER,
        user=DEST_USER,
        password=os.environ[DEST_PASSWORD_VARIABLE],
    )
    source_conn.autocommit = True
    dest_conn.autocommit = True

    create_tables(dest_conn)

    with source_conn.cursor(cursor_factory=DictCursor) as source_cursor, dest_conn.cursor(
        cursor_factory=DictCursor
    ) as dest_cursor:
        for table, table_info in TABLES.items():
            # Read data from source
            source_cursor.execute(f"select * from {table}")

            # transform the data
            for row in source_cursor.fetchall():
                for field in table_info.fields_to_hash:
                    row[field] = hashlib.sha256(str(row[field]).encode("utf-8")).hexdigest()

                # write transformed data to dest db
                insert_query = insert_statement_from_dict(row, table)
                dest_cursor.execute(insert_query)


if __name__ == "__main__":
    pipeline()

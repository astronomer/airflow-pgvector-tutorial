"""
## Delete a table in Postgres

This DAG deletes a table in Postgres. It is used to clean up the table
created by the `query_book_vectors` DAG during development.
"""

from airflow.sdk import dag
from pendulum import datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator


@dag(
    start_date=datetime(2025, 8, 1),
    schedule=None,
)
def delete_table():
    PostgresOperator(
        task_id="delete_table",
        postgres_conn_id="postgres_default",
        sql="DROP TABLE IF EXISTS Book",
    )


delete_table()

from airflow.decorators import dag
from pendulum import datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator


@dag(
    start_date=datetime(2023, 9, 1),
    schedule=None,
    catchup=False,
)
def delete_table():
    PostgresOperator(
        task_id="delete_table",
        postgres_conn_id="postgres_default",
        sql="DROP TABLE IF EXISTS Book",
    )


delete_table()

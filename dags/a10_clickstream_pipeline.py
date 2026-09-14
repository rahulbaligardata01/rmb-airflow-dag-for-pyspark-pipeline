from airflow.sdk import dag, task
from datetime import datetime


@dag(
    dag_id="a10_clickstream_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["a10", "clickstream", "pyspark"],
)
def a10_clickstream_pipeline():

    @task
    def extract():
        print("Extract task started")
        return "raw clickstream data"

    @task
    def validate(data):
        print(f"Validate task received: {data}")
        return "validated clickstream data"

    extracted_data = extract()
    validate(extracted_data)


a10_clickstream_pipeline()
from datetime import datetime, timedelta
import os
import subprocess

from airflow.sdk import dag, task
from airflow.providers.standard.sensors.filesystem import FileSensor

@dag(
    dag_id="a10_clickstream_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["a10", "clickstream", "pyspark"],
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
)
def a10_clickstream_pipeline():

    wait_for_file = FileSensor(
        task_id="wait_for_file",
        filepath="clickstream_01.json",
        fs_conn_id="fs_default",
        poke_interval=10,
        timeout=300,
    )

    @task
    def extract():
        input_path = "/opt/airflow/data/input"
        print(f"Extract: reading from {input_path}")
        return input_path

    @task
    def validate(input_path):
        print(f"Validate: checking {input_path}")

        if not os.path.exists(input_path):
            raise FileNotFoundError(
                f"Input directory does not exist: {input_path}"
            )

        return {
            "input_path": input_path,
            "status": "VALID",
        }

    @task
    def list_input_files(validation_metadata):
        input_path = validation_metadata["input_path"]

        files = [
            os.path.join(input_path, filename)
            for filename in os.listdir(input_path)
            if filename.endswith(".json")
        ]

        if not files:
            raise FileNotFoundError(
                f"No JSON input files found in: {input_path}"
            )

        print(f"Found {len(files)} input files:")

        for file_path in files:
            print(file_path)

        return files

    @task
    def process_file(file_path):
        print(f"Processing file: {file_path}")
        return file_path

    @task
    def transform(validation_metadata, processed_files):
        input_path = validation_metadata["input_path"]

        print(f"Transform: processing {input_path}")
        print(f"Processed files: {processed_files}")

        subprocess.run(
            [
                "spark-submit",
                "/opt/airflow/spark/clickstream_transform.py",
            ],
            check=True,
        )

        return "/opt/airflow/data/output"

    @task
    def load(output_path):
        print(f"Load: loading {output_path}")
        return output_path

    @task
    def data_quality(output_path):
        print(f"Data quality: checking {output_path}")

        if not os.path.exists(output_path):
            raise FileNotFoundError(
                f"Output directory does not exist: {output_path}"
            )

        parquet_files = []

        for root, _, files in os.walk(output_path):
            for filename in files:
                if filename.endswith(".parquet"):
                    parquet_files.append(
                        os.path.join(root, filename)
                    )

        if not parquet_files:
            raise ValueError(
                f"No Parquet files found in: {output_path}"
            )

        print(f"Data quality passed.")
        print(f"Parquet files found: {len(parquet_files)}")

        return {
            "status": "PASSED",
            "output_path": output_path,
            "parquet_file_count": len(parquet_files),
        }

    @task
    def notify(dq_result):
        if dq_result["status"] == "PASSED":
            print("Notify: pipeline completed successfully")
            print(f"Output path: {dq_result['output_path']}")
            print(
                f"Parquet files: {dq_result['parquet_file_count']}"
            )
        else:
            raise ValueError("Data quality checks failed")

    @task(retries=2, retry_delay=timedelta(seconds=10)) # Short delay for retry testing, test will be faster
    def test_retry():
        print("Testing Airflow retry behavior")

        raise ValueError("Intentional failure for retry testing")

    input_path = extract()

    wait_for_file >> input_path

    validated_path = validate(input_path)
    input_files = list_input_files(validated_path)

    processed_files = process_file.expand(file_path=input_files)

    output_path = transform(validated_path, processed_files)
    loaded_path = load(output_path)
    dq_result = data_quality(loaded_path)
    notify(dq_result)

    test_retry()


a10_clickstream_pipeline()
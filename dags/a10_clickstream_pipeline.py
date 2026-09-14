from datetime import datetime, timedelta
import os

from airflow.sdk import dag, task


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

        return input_path

    @task
    def list_input_files(input_path):
        files = [
            os.path.join(input_path, file)
            for file in os.listdir(input_path)
            if file.endswith(".json")
        ]

        if not files:
            raise FileNotFoundError(
                f"No JSON input files found in: {input_path}"
            )

        print(f"Found {len(files)} input files:")
        for file in files:
            print(file)

        return files

    @task
    def process_file(file_path):
        print(f"Processing file: {file_path}")
        return file_path

    @task
    def transform(input_path):
        print(f"Transform: processing {input_path}")
        return "/opt/airflow/data/output"

    @task
    def load(output_path):
        print(f"Load: loading {output_path}")
        return output_path

    @task
    def data_quality(output_path):
        print(f"Data quality: checking {output_path}")
        return True

    @task
    def notify(dq_passed):
        if dq_passed:
            print("Notify: pipeline completed successfully")
        else:
            raise ValueError("Data quality checks failed")

    input_path = extract()
    validated_path = validate(input_path)
    input_files = list_input_files(validated_path)

    processed_files = process_file.expand(file_path=input_files)

    output_path = transform(validated_path)
    loaded_path = load(output_path)
    dq_result = data_quality(loaded_path)
    notify(dq_result)


a10_clickstream_pipeline()
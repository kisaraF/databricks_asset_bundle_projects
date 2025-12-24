import requests
from requests import exceptions as req_errors
import json
import logging
from datetime import datetime
from databricks.connect import DatabricksSession
from pyspark.sql.functions import (
    try_parse_json,
    lit,
    col,
    sha2,
    current_timestamp,
    to_timestamp,
)


def get_spark_session() -> DatabricksSession:
    spark = DatabricksSession.builder.serverless().profile("DEFAULT").getOrCreate()
    logging.info("Spark Session Created!")
    return spark


def retrieve_construct(skip_i: int) -> str:
    joined_url = (
        f"https://api.fda.gov/animalandveterinary/event.json?skip={skip_i}&limit=100"
    )
    logging.info(f"Link created as {joined_url}")
    return joined_url


def get_data(url: str) -> list[dict]:
    try:
        res = requests.get(url)
        res.raise_for_status()
        data = res.json()
        logging.info("Results successfully extracted")
        return data["results"]
    except req_errors.HTTPError as err:
        logging.error(f"HTTP error -> {err}")
    except req_errors.RequestException as req_err:
        logging.error(f"Request error -> {req_err}")


# WIP
def check_hash_cols(
    to_check: list[str], table_name: str, spark: DatabricksSession
) -> list[str]:
    """
    Given a list of hash ids, check if they exist in the
    passed table
    """
    pass


def insert_to_table(
    data_json: list[dict], spark: DatabricksSession, tgt_table: str
) -> None:
    rows = [
        json.dumps(r) for r in data_json
    ]  # This will make sure that inconsistent data for each keys in the JSON will correctly get inserted

    df = spark.createDataFrame(rows, "string").toDF("raw_payload")

    df_parsed = df.withColumns(
        {
            "_hash_id": sha2(col("raw_payload"), 224),
            "variant_payload": try_parse_json(col("raw_payload")),
            "valid_from": current_timestamp(),
            "valid_to": to_timestamp(lit("9999-12-31 00:00:00"), "yyyy-MM-dd HH:mm:ss"),
        }
    )

    df_parsed = df_parsed.select(
        "variant_payload", "_hash_id", "valid_from", "valid_to"
    )

    df_parsed.write.mode("append").saveAsTable(tgt_table)
    logging.info("Dataframe written to table")


def load_data(catalog_name: str, schema_name: str, table_name: str, load_n: int):
    # Creating the logger
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )

    spark = get_spark_session()

    table_name = f"{catalog_name}.{schema_name}.{table_name}"

    for i in range(1, load_n, 100):
        url_to_pass = retrieve_construct(i)
        results = get_data(url_to_pass)
        insert_to_table(results, spark, table_name)

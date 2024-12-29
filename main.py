import os
from dotenv import load_dotenv, find_dotenv
import yaml

from src.os.read import get_incoming_data, prep_incoming_data
from src.database.models import DWHSchema, BankSchema
from src.database.clients import DWHClient, BankDBClient

if __name__ == "__main__":
    load_dotenv(find_dotenv())

    with open("configs/os/files.yaml", "r") as os_cfg:
        os_cfg = yaml.safe_load(os_cfg)
    with open("configs/database/dwh.yaml", "r") as dwh_cfg:
        dwh_cfg = yaml.safe_load(dwh_cfg)

    # Establish connection with bank database
    bank_schema = BankSchema.from_yaml("configs/database/bank.yaml")
    bank_client = BankDBClient(
        database=os.getenv("DB_NAME"),
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        port=os.getenv("DB_PORT"),
        schema=bank_schema
    )

    # Establish connection with data warehouse
    dwh_schema = DWHSchema.from_yaml("configs/database/dwh.yaml")
    dwh_client = DWHClient(
        database=os.getenv("DB_NAME"),
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        port=os.getenv("DB_PORT"),
        schema=dwh_schema,
        scd2_config=dwh_cfg["scd2"],
        fact_mapping=dwh_cfg["fact_mapping"]
    )

    # Initialize data warehouse schema
    dwh_client.create_schema("main.ddl")

    # Insert bank data to tables
    dwh_client.insert_bank_tables(bank_client)

    # Get incoming data
    incoming_data = get_incoming_data(os_cfg["data_dir"], os_cfg["patterns"])

    incoming_data = prep_incoming_data(incoming_data, os_cfg["preprocess"])

    for date, data in incoming_data.items():

        # Insert incoming data to tables
        dwh_client.insert_incoming_tables(data, date)

        # Report frauds
        dwh_client.report_frauds()
from loguru import logger
import os
import requests
import pandas as pd
import numpy as np
from src.Data import DATA_DIR
import json
import datetime
from datetime import date, datetime, timedelta
import re
import csv
from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")

collections_path = ["Transactions.csv", "Units.csv", "transactions-2023.csv"]

db = client["PropertyGPT"]
Transactions_collection = db["Transactions"]
Units_collection = db["Units"]
DLD_transaction_collection = db["transactions-2023"]

Unit_exclude = [
    "property_id",
    "area_id",
    "area_name_ar",
    "land_number",
    "land_sub_number",
    "parking_allocation_type",
    "parking_allocation_type_ar",
    "parking_allocation_type_en",
    "rooms_ar",
    "property_type_ar",
    "property_type_en",
    "property_sub_type_id",
    "property_sub_type_ar",
    "creation_date",
    "munc_zip_code",
    "pre_registration_number",
    "master_project_ar",
    "land_type_ar",
    "zone_id",
]

Transactions_exclude = [
    "transaction_id",
    "trans_group_id",
    "trans_group_ar",
    "procedure_name_ar",
    "property_type_id",
    "property_type_ar",
    "property_sub_type_id",
    "property_sub_type_ar",
    "property_usage_ar",
    "reg_type_id",
    "reg_type_ar",
    "reg_type_en",
    "area_name_ar",
    "building_name_ar",
    "project_name_ar",
    "master_project_ar",
    "nearest_landmark_ar",
    "nearest_metro_ar",
    "nearest_mall_ar",
    "rooms_ar",
]

DLD_transaction_exclude = ["Transaction Number"]

excluded_columns = [Transactions_exclude, Unit_exclude, DLD_transaction_exclude]

list_of_collections = [
    Transactions_collection,
    Units_collection,
    DLD_transaction_collection,
]


def convert_to_correct_type(key, value):
    if key == "instance_date" or key == "Transaction Date":
        try:
            date_format = "%m/%d/%Y %H:%M"
            datetime_obj = datetime.strptime(value, date_format)

            # Extract the year, month, and day from the datetime object
            year = datetime_obj.year
            month = datetime_obj.month
            day = datetime_obj.day

            month = str(month).zfill(2)
            day = str(day).zfill(2)

            return f"{year}-{month}-{day}"
        except ValueError:
            # print("error")
            # print(type(value))
            # print(value)
            return value
    elif key == "Amount":
        return float(value)
    elif key == "floor":
        return str(value)
    else:
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value


for n in range(len(list_of_collections)):
    logger.info(f"Adding {collections_path[n]} to Mongo Database ...")

    if (
        collections_path[n] == "Transactions.csv"
        or collections_path[n] == "transactions-2023.csv"
    ):
        logger.info("Passed")
        continue
    with open(DATA_DIR / collections_path[n], "r") as file:
        reader = csv.reader(file)
        header = next(reader)  # Read the header row

        header[12] = "Property Size (sq_m)"
        print(header)

        included_indices = [
            j for j, col in enumerate(header) if col not in excluded_columns[n]
        ]

        batch_size = 1  # Number of documents to insert at once
        batch = []  # Batch of documents

        for row in reader:
            # Filter the row to include only the columns to be inserted
            try:
                # Filter the row to include only the columns to be inserted
                filtered_row = [row[i] for i in included_indices]

                # Convert each row to the correct data type
                document = {
                    header[i]: convert_to_correct_type(header[i], value)
                    for i, value in zip(included_indices, filtered_row)
                }
                batch.append(document)

                if len(batch) == batch_size:
                    try:
                        list_of_collections[n].insert_many(batch)
                    except:
                        column_index = next(
                            (i for i, val in enumerate(row) if val), None
                        )
                        if column_index is not None:
                            column_name = header[included_indices[column_index]]
                            logger.error(f"OverflowError in column: {column_name}")
                        logger.warning("Skipping batch due to OverflowError")
                    batch = []

            except OverflowError as e:
                column_index = next((i for i, val in enumerate(row) if val), None)
                if column_index is not None:
                    column_name = header[included_indices[column_index]]
                    logger.error(f"OverflowError in column: {column_name}")
                raise e
        # Insert any remaining documents
        if batch:
            list_of_collections[n].insert_many(batch)

        logger.info(
            f"Adding {collections_path[n]} to Mongo Database Done Successfully !!!"
        )
# Close the MongoDB connection
client.close()

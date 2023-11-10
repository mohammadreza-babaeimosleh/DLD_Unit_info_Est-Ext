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


# Establish a connection to MongoDB
client = MongoClient("mongodb://localhost:27017/")

# Access the desired database and collection
db = client["PropertyGPT"]

Transactions_collection = db["Transactions"]
Units_collection = db["Units"]
DLD_transaction_collection = db["transactions-2023"]


def Pulse_data_extractor(
    building_name, area_name, no_bedrooms, square_footage, floor, valid_passed_month=12
):
    # Calculate the date threshold
    passed_month = datetime.now() - timedelta(days=valid_passed_month * 30)

    passed_month_str = passed_month.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    # Query MongoDB using PyMongo
    query = {
        # "building_name_en": {"$regex": re.compile(building_name, re.IGNORECASE)},
        "area_name_en": {"$regex": re.compile(area_name, re.IGNORECASE)},
        "rooms_en": {"$regex": re.compile(no_bedrooms, re.IGNORECASE)},
        # "procedure_area": {"$eq": square_footage},
        "$expr": {
            "$gt": [
                {"$dateFromString": {"dateString": "$instance_date"}},
                {"$dateFromString": {"dateString": passed_month_str}},
            ]
        },
    }
    return_cursor = Transactions_collection.find(query)
    # Convert the cursor to a list of dictionaries
    return_data = list(return_cursor)
    # Close the MongoDB connection

    # Convert the list of dictionaries to a DataFrame (if needed)
    return_df = pd.DataFrame(return_data)

    # print(return_df["actual_worth"])

    return return_df


def data_extractor(
    building_name, area_name, no_bedrooms, square_footage, floor, valid_passed_month=12
):
    logger.info("Extracting info from Pulse_dataset...")

    results = []

    results1 = Pulse_data_extractor(
        building_name=building_name,
        area_name=area_name,
        no_bedrooms=no_bedrooms,
        square_footage=square_footage,
        floor=floor,
        valid_passed_month=valid_passed_month,
    )
    results.append(results1)

    # print(results[0])

    if len(results[0]) != 0:
        logger.info(f"{results[0].shape[0]} Records found in Pulse_dataset")

        logger.info("Features extracted from Pulse_dataset!!!")

        # print(results[0]["actual_worth"])

        return results

    else:
        logger.info("Nothing found!!!")
        return None


def data_call(
    building_name, area_name, no_bedrooms, square_footage, floor, valid_passed_month=12
):
    search_results = []
    try:
        logger.warning(f"Searching for building with {no_bedrooms} on floor: {floor}")
        res = data_extractor(
            building_name=building_name,
            area_name=area_name,
            no_bedrooms=no_bedrooms,
            square_footage=square_footage,
            floor=floor,
            valid_passed_month=valid_passed_month,
        )
        search_results.append(res)
    except:
        logger.error("There was an error searching in dataframes for your data")
    return search_results


def Unit_price_estimator(search_results):
    num_data = 0
    value_sum = 0
    for res in search_results:
        if res == None:
            continue
        value_sum += res[0]["meter_sale_price"].sum()
        num_data += res[0].shape[0]

    for df in search_results:
        if df == None:
            continue
        actual_area = df[0]["procedure_area"].sum()
    actual_area = actual_area / num_data
    return_value_per_meter = value_sum / num_data
    return_value = round(actual_area * return_value_per_meter, 2)

    logger.warning(f"{num_data} Records was founded")

    return return_value


if __name__ == "__main__":
    logger.warning(
        "Please enter your data\nYour data has to be in the following order:\n Building name (str),\n Area name (str),\n # of bedrooms (int),\n Sqaure footage (float),\n Floor,\n Valid passed month to search (int)\n Note: Your data has te seprated by ',' without any spaces from both left and right side"
    )
    input_data = input("")

    input_data = input_data.split(",")

    input_dict = {
        "building_name": input_data[0],
        "area_name": input_data[1],
        "no_bedrooms": input_data[2],
        "square_footage": float(input_data[3]),
        "floor": input_data[4],
        "valid_passed_month": int(input_data[5]),
    }

    results = data_call(
        building_name=input_dict["building_name"],
        area_name=input_dict["area_name"],
        no_bedrooms=input_dict["no_bedrooms"],
        square_footage=input_dict["square_footage"],
        floor=input_dict["floor"],
        valid_passed_month=input_dict["valid_passed_month"],
    )

    print(Unit_price_estimator(results))
    # print(results[2])

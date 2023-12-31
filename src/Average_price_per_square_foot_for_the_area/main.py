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

# List of needed Datasets
df_list = ["Transactions.csv", "transactions-2023-09-05.csv", "Units.csv"]


# Load data
def batch_data_loader(filename, batch_size=1000000):
    df = pd.DataFrame()
    for chunk in pd.read_csv(DATA_DIR / filename, chunksize=batch_size):
        df = pd.concat([df, chunk])

    return df


# Remove firs character of Transaction Id in Pulse dataset
def Pulse_id_edittor(entry):
    ind = entry.find("-")
    entry = entry[ind + 1 :]
    return entry


# Extracting data from Pulse dataset
def Pulse_data_extractor(
    building_name, area_name, no_bedrooms, square_footage, floor, valid_passed_month=12
):
    passed_month = datetime.now() - timedelta(days=valid_passed_month * 30)

    Pulse_transactions_df["instance_date"] = pd.to_datetime(
        Pulse_transactions_df["instance_date"]
    )

    return_df = Pulse_transactions_df[
        # (Pulse_transactions_df["building_name_en"].str.lower() == building_name.lower())
        (Pulse_transactions_df["area_name_en"].str.lower() == area_name.lower())
        & (Pulse_transactions_df["rooms_en"].str.lower() == no_bedrooms.lower())
        # & (Pulse_transactions_df["procedure_area"] == square_footage)
        & (Pulse_transactions_df["instance_date"] > passed_month)
    ]

    return_df["instance_date"] = return_df["instance_date"].dt.date

    return return_df


# Function to extract all needed data
def data_extractor(
    building_name, area_name, no_bedrooms, square_footage, floor, valid_passed_month=12
):
    logger.info("Extracting info from Pulse_dataset...")

    final_data = Pulse_data_extractor(
        building_name=building_name,
        area_name=area_name,
        no_bedrooms=no_bedrooms,
        square_footage=square_footage,
        floor=floor,
        valid_passed_month=valid_passed_month,
    )

    # print(final_data.shape)
    logger.info(f"{final_data.shape[0]} Records found in Pulse_dataset")

    logger.info("Features extracted from Pulse_dataset!!!")

    return final_data


# Handling different desiered data
def data_call(
    building_name, area_name, no_bedrooms, square_footage, floor, valid_passed_month=12
):
    search_results = []
    try:
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


# Estimating unit price
def Unit_price_estimator(search_results):
    num_data = 0
    value_sum = 0
    for res in search_results:
        if res.empty:
            continue
        value_sum += res["meter_sale_price"].sum()
        num_data += res.shape[0]

    for df in search_results:
        if df.empty:
            continue
        actual_area = df["procedure_area"].sum()
    actual_area = actual_area / num_data
    return_value_per_meter = value_sum / num_data
    return_value = round(actual_area * return_value_per_meter, 2)

    logger.warning(f"{num_data} Records was founded")

    return return_value


logger.info("Loading Pulse_transactions dataset...")

# Loading Pulse data
Pulse_transactions_df = batch_data_loader(
    df_list[0],
)
# Needed columns of Pulse data
Pulse_df_features = [
    "transaction_id",
    "instance_date",
    "area_name_en",
    "building_name_en",
    "project_name_en",
    "master_project_en",
    "nearest_landmark_en",
    "nearest_metro_en",
    "nearest_mall_en",
    "rooms_en",
    "has_parking",
    "procedure_area",
    "actual_worth",
    "meter_sale_price",
]

# Excluding unwanted columns
Pulse_df_droplist = [
    x for x in Pulse_transactions_df.columns.tolist() if (x not in Pulse_df_features)
]
Pulse_transactions_df = Pulse_transactions_df.drop(Pulse_df_droplist, axis=1)

logger.info("Pulse_transactions dataset loaded successfully!!!")

# Preproceccing needed to start scanning data
Pulse_transactions_df["transaction_id"] = Pulse_transactions_df["transaction_id"].apply(
    Pulse_id_edittor
)


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

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
        "building_name_en": {"$regex": re.compile(building_name, re.IGNORECASE)},
        "area_name_en": {"$regex": re.compile(area_name, re.IGNORECASE)},
        "rooms_en": {"$regex": re.compile(no_bedrooms, re.IGNORECASE)},
        "procedure_area": {"$eq": square_footage},
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

    return return_df


def DLD_parking_extractor(results, DLD_transactions_collection):
    results[0]["instance_date"] = pd.to_datetime(
        results[0]["instance_date"]
    ).dt.strftime("%Y-%m-%d")

    data_list = results[0].to_dict(orient="records")
    # [["rooms_en", "procedure_area", "actual_worth", "instance_date"]]
    result_list = []
    for data in data_list:
        filter_conditions = {
            "$and": [
                {
                    "Room(s)": data["rooms_en"],
                    "Property Size (sq_m)": data["procedure_area"],
                    "Amount": data["actual_worth"],
                    "Transaction Date": data["instance_date"],
                }
            ]
        }

        result = list(DLD_transactions_collection.find(filter_conditions))
        if len(result) != 0:
            for ind, res in enumerate(result):
                res["building_name_en"] = data["building_name_en"]
                res["area_name_en"] = data["area_name_en"]
                res["meter_sale_price"] = data["meter_sale_price"]
                result[ind] = res

            result_list.append(result)

    result_list = pd.DataFrame(result_list)

    return result_list


def Unit_extractor(results, floor, Units):
    data_list = results[1].to_dict(orient="records")
    result_list = []
    for data in data_list:
        filter_conditions = {
            "$and": [
                {
                    "unit_parking_number": data[0]["Parking"],
                    "area_name_en": {"$eq": data[0]["area_name_en"]},
                    "rooms_en": data[0]["Room(s)"],
                    "actual_area": data[0]["Property Size (sq_m)"],
                    "floor": floor,
                }
            ]
        }

        result = list(Units.find(filter_conditions))

        if len(result) != 0:
            for ind, res in enumerate(result):
                res["building_name_en"] = data[0]["building_name_en"]
                res["area_name_en"] = data[0]["area_name_en"]
                res["meter_sale_price"] = data[0]["meter_sale_price"]
                result[ind] = res

            result_list = result_list + result

    # Convert the result to a pandas DataFrame
    selected_df = pd.DataFrame(result_list)
    return selected_df

    # Close the MongoDB connection


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

    logger.info(f"{results[0].shape[0]} Records found in Pulse_dataset")

    logger.info("Features extracted from Pulse_dataset!!!")

    if len(results[0]) != 0:
        logger.info("Extracting info from DLD_dataset...")

        results2 = DLD_parking_extractor(results, DLD_transaction_collection)
        results.append(results2)

        # print(results[1])

        logger.info(f"{results[1].shape[0]} Records found in DLD_dataset")

        logger.info("Features extracted from DLD_dataset!!!")

        if len(results[1]) != 0:
            logger.info("Extracting info from Units_dataset...")

            results3 = Unit_extractor(results, floor, Units_collection)
            results.append(results3)

            if len(results[2]) != 0:
                # print(results[2][["area_name_en", "floor"]])

                logger.info(f"{results[2].shape[0]} Records found in Units_dataset")

                logger.info("Features extracted from Units_dataset!!!")

                return results

            else:
                logger.info("Going to next object!!!")
                return None

        else:
            logger.info("Going to next object!!!")
            return None
    else:
        logger.info("Going to next object!!!")
        return None


def data_call(
    building_name, area_name, no_bedrooms, square_footage, floor, valid_passed_month=12
):
    valid_rooms = [
        "Studio",
        "1 B/R",
        "2 B/R",
        "3 B/R",
        "4 B/R",
        "5 B/R",
        "6 B/R",
        "7 B/R",
        "8 B/R",
        "9 B/R",
        "10 B/R",
    ]

    valid_floors = ["G"] + list(map(str, range(1, 41)))

    rooms_to_search = []
    floors_to_search = []

    if no_bedrooms == "Studio":
        index = valid_rooms.index(no_bedrooms)
        rooms_to_search.append(no_bedrooms)
        rooms_to_search.append(valid_rooms[index + 1])
    elif no_bedrooms == "10 B/R":
        index = valid_rooms.index(no_bedrooms)
        rooms_to_search.append(no_bedrooms)
        rooms_to_search.append(valid_rooms[index - 1])
    elif no_bedrooms in valid_rooms:
        index = valid_rooms.index(no_bedrooms)
        for ind in range(index - 1, index + 2):
            rooms_to_search.append(valid_rooms[ind])
    else:
        rooms_to_search.append(no_bedrooms)

    if floor == "G":
        index = valid_floors.index(floor)
        floors_to_search.append(floor)
        floors_to_search.append(valid_floors[index + 1])
        floors_to_search.append(valid_floors[index + 2])
    elif floor == 40:
        index = valid_floors.index(floor)
        floors_to_search.append(floor)
        floors_to_search.append(valid_floors[index - 1])
        floors_to_search.append(valid_floors[index - 2])
    elif floor == 1:
        index = valid_floors.index(floor)
        floors_to_search.append(floor)
        floors_to_search.append(valid_floors[index - 1])
        floors_to_search.append(valid_floors[index + 1])
        floors_to_search.append(valid_floors[index + 2])
    elif floor == 39:
        index = valid_floors.index(floor)
        floors_to_search.append(floor)
        floors_to_search.append(valid_floors[index + 1])
        floors_to_search.append(valid_floors[index - 1])
        floors_to_search.append(valid_floors[index - 2])
    elif floor in valid_floors:
        index = valid_floors.index(floor)
        for ind in range(index - 2, index + 3):
            floors_to_search.append(valid_floors[ind])
    else:
        floors_to_search.append(floor)

    # try:
    search_results = []
    for num in rooms_to_search:
        for fl in floors_to_search:
            logger.warning(f"Searching for building with {num} on floor: {fl}")
            res = data_extractor(
                building_name=building_name,
                area_name=area_name,
                no_bedrooms=num,
                square_footage=square_footage,
                floor=fl,
                valid_passed_month=valid_passed_month,
            )
            search_results.append(res)
    # except:
    #     logger.error("There was an error searching in dataframes for your data")

    return search_results


def Unit_price_estimator(search_results):
    num_data = 0
    value_sum = 0
    actual_area = 0
    for res in search_results:
        if res == None:
            continue
        value_sum += res[2]["meter_sale_price"].sum()
        num_data += res[2].shape[0]

    for df in search_results:
        if df == None:
            continue
        actual_area = df[2]["actual_area"].sum()

    try:
        actual_area = actual_area / num_data
        return_value_per_meter = value_sum / num_data
        return_value = round(actual_area * return_value_per_meter, 2)
    except:
        return_value = "price not found"

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

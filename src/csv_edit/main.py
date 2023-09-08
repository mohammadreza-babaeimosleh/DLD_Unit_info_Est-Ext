import pandas as pd
from loguru import logger

from src.Data import DATA_DIR

valid_inputs = [1, 2, 3]

while True:
    i = int(
        input(
            "Please enter one of the mentioned number to access your desiered data:\n1 - DLD_transaction data\n2 - DLD_RENT_CONTRACTS data\n"
        )
    )
    if i in valid_inputs:
        i = i - 1
        break
    else:
        logger.error("Error: Invalid number please try again")

logger.info("Loading old dataset...")

df_list = ["Transactions.csv", "Rent_Contracts.csv", "Units.csv"]
date_instance_list = ["instance_date", "contract_start_date", "creation_date"]

# Adjust this value based on your system's memory capacity
chunk_size = 1000000

old_transactions_df = pd.DataFrame()


for chunk in pd.read_csv(DATA_DIR / df_list[i], chunksize=chunk_size):
    old_transactions_df = pd.concat([old_transactions_df, chunk])

logger.info("Old dataset loaded successfully!!!")

logger.info("editting transactions file...")

print(old_transactions_df[date_instance_list[i]].head())

# Converting all data in column <instance_date> to date format for removing useless data
old_transactions_df[date_instance_list[i]] = pd.to_datetime(
    old_transactions_df[date_instance_list[i]], errors="coerce"
)
mask = old_transactions_df[date_instance_list[i]].isnull()
old_transactions_df = old_transactions_df[~mask]

# Sorting by decending order
old_transactions_df = old_transactions_df.sort_values(
    date_instance_list[i], ascending=False
)

logger.info("editting transactions done!!!")

logger.info("saving editted file...")

old_transactions_df.to_csv(DATA_DIR / df_list[i], mode="w", index=False)

logger.info("editted file saved successfuly!!!")

Code for extracting price and information of a Unit based on provided data

# Requirements
To install requirements, run the command below in your terminal
```bash
pip install -r requirements.txt
```
# How to Use 
To use the code below without any security issues run simply these instructions:
1. Run the command below in the current working directory (project folder with src folder included)

```bash
    export PYTHONPATH=${PWD}
```
2. Download [Pulse Transactions](https://www.dubaipulse.gov.ae/data/dld-transactions/dld_transactions-open), [Units](https://www.dubaipulse.gov.ae/data/dld-registration/dld_units-open), [DLD Transactions](https://dubailand.gov.ae/en/open-data/real-estate-data/#/) datasets and place it in **Data** folder in ./src path 

*note-1*: do not rename the dataset file 

*note-2*: you need to edit your code based on the name of your downloaded[DLD Transactions](https://dubailand.gov.ae/en/open-data/real-estate-data/#/) dataet

3. Datasets provided by DLD may have some structural issues. To solve them run the code provided in the csv_edit folder by using the below command (optional and may need code editting)
```bash
    python ./src/csv_edit/main.py
```

4. run the API call code for receiving the newest data like below:
```bash
    python python ./src/DLD_to_Pulse/main.py
```
5. After some logs of processing code requests for entry. Provide code with valid data in mentioned format

the output of code would the price of your desired unit

import pandas as pd
from dagster import asset
import os


@asset(
    compute_kind="Pandas",
    description="Read CSV with categories",
)
def categories():
    file_path = "/Users/sspaeti/Simon/Sync/1 Areas/Finance/Bank/Kontoauszüge/_analytics/manual_categories.csv"

    # Reading the CSV file
    df = pd.read_csv(file_path, sep=",", engine="python")

    # Renaming columns
    df.columns = [
        "cat",
        "sub_cat",
        "BookingText",
        "NameOriginatorBeneficiary",
    ]

    return df


@asset(
    compute_kind="Pandas",
    description="Read CSV file from expoted BEKB transactions (convert XLSX to CSV first)",
)
def transactions():
    ##
    ## One file only
    ##
    # file_path = "/Users/sspaeti/Simon/Sync/1 Areas/Finance/Bank/Kontoauszüge/_export/_Transaktionen_Export/Test-2024/2024_test.csv"

    # # Reading the CSV file
    # df = pd.read_csv(file_path, sep=",", engine="python")

    # # Renaming columns
    # df.columns = [
    #     "CreditDebit",
    #     "Date",
    #     "ValueDate",
    #     "BookingText",
    #     "AdditionalInfoBooking",
    #     "NameOriginatorBeneficiary",
    #     "AddressOriginatorBeneficiary",
    #     "AccountBank",
    #     "MessageReference",
    #     "AdditionalInfoTransaction",
    #     "Amount",
    #     "Balance",
    # ]

    # # Converting 'Date' and 'ValueDate' to datetime
    # df["Date"] = pd.to_datetime(df["Date"], format="%d.%m.%Y")
    # df["ValueDate"] = pd.to_datetime(df["ValueDate"], format="%d.%m.%Y")

    # # Converting 'Amount' to a numeric type
    # df["Amount"] = pd.to_numeric(df["Amount"], errors="coerce")

    # Folder path
    folder_path = "/Users/sspaeti/Simon/Sync/1 Areas/Finance/Bank/Kontoauszüge/_export/BEKB/CSVs/"

    # Function to process each CSV file
    def process_csv(file_path):
        df = pd.read_csv(file_path, sep=",", engine="python")
        df.columns = [
            "CreditDebit",
            "Date",
            "ValueDate",
            "BookingText",
            "AdditionalInfoBooking",
            "NameOriginatorBeneficiary",
            "AddressOriginatorBeneficiary",
            "AccountBank",
            "MessageReference",
            "AdditionalInfoTransaction",
            "Amount",
            "Balance",
        ]
        df["Date"] = pd.to_datetime(df["Date"], format="%d.%m.%Y")
        df["ValueDate"] = pd.to_datetime(df["ValueDate"], format="%d.%m.%Y")
        df["Amount"] = pd.to_numeric(df["Amount"], errors="coerce")
        return df

    # Loop through each CSV file in the folder
    all_data = []
    for filename in os.listdir(folder_path):
        if filename.endswith(".csv"):
            file_path = os.path.join(folder_path, filename)
            df = process_csv(file_path)
            all_data.append(df)

    # Combine all data into a single DataFrame
    combined_data = pd.concat(all_data, ignore_index=True)

    return combined_data

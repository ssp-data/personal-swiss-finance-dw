import pandas as pd
from dagster import asset
import os

from datetime import datetime

from dagster_pandas import PandasColumn, create_dagster_pandas_dataframe_type


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


def compute_transaction_dataframe_summary_statistics(dataframe):
    return {
        "min_transaction_date": min(dataframe["Date"]).strftime("%Y-%m-%d"),
        "max_transaction_date": max(dataframe["Date"]).strftime("%Y-%m-%d"),
        # # If you have a merchant name column, replace 'MerchantName' with the correct column name
        # "unique_merchants": str(dataframe["MerchantName"].nunique())
        # if "MerchantName" in dataframe.columns
        # else "N/A",
        "n_rows": len(dataframe),
        "sum_amount": sum(dataframe["Amount"]),
        "columns": str(dataframe.columns),
    }


BekbTransactionDataFrame = create_dagster_pandas_dataframe_type(
    name="BekbTransactionDataFrame",
    columns=[
        PandasColumn.categorical_column(
            "CreditDebit", categories={"Gutschrift", "Belastung"}
        ),
        PandasColumn.datetime_column(
            "Date", min_datetime=datetime(year=2000, month=1, day=1)
        ),
        PandasColumn.datetime_column(
            "ValueDate", min_datetime=datetime(year=2000, month=1, day=1)
        ),
        PandasColumn.string_column("BookingText"),
        PandasColumn.string_column("AdditionalInfoBooking"),
        PandasColumn.string_column("NameOriginatorBeneficiary"),
        PandasColumn.string_column("AddressOriginatorBeneficiary"),
        PandasColumn.string_column("AccountBank"),
        PandasColumn.string_column("MessageReference"),
        PandasColumn.string_column("AdditionalInfoTransaction"),
        PandasColumn.float_column("Amount"),
        PandasColumn.float_column("Balance"),
    ],
    metadata_fn=compute_transaction_dataframe_summary_statistics,
)


@asset(
    compute_kind="Pandas",
    description="Read CSV file from expoted BEKB transactions (convert XLSX to CSV first)",
    dagster_type=BekbTransactionDataFrame,
)
def transactions():
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

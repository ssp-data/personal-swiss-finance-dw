from dagster import (
    solid,
    op,
    String,
    OutputDefinition,
    LocalFileHandle,
    OutputDefinition,
    Out,
)
from banking.types.pandas import (
    BekbTransactionDataFrame,
    TransactionDataFrame,
)
from banking.types.general import XmlType, JsonType

from pandas import DataFrame, read_csv
from dagster.utils import script_relative_path
import pandas as pd
import numpy as np
import io
import os
import zipfile
import base64
import xml.etree.ElementTree as ET


@op(
    required_resource_keys={"bekb_client"},
    description="This operation will download transactions from BEKB to json",
)
def download_transactions(
    context,
    iban: String,
    language: String,
    date_from: String,
    date_to: String,
    copy: String,
) -> JsonType:
    context.log.info(
        "start connecting to BEKB and waiting for smartlogin confirmation.."
    )

    bekb_client = context.resources.bekb_client
    session = bekb_client.smartlogin_connection()
    json_response = bekb_client.fetch_camt053(iban, language, date_from, date_to, copy)

    context.log.info("camt053 transactions successfully donwloaded")
    bekb_client.close_connection()

    number_transaction = json_response["anzahl_auszuege"]
    return decompress_and_uncompress(
        json_response["camt053_datei_inhalt"], json_response["camt053_datei_name"]
    )


def decompress_and_uncompress(request_string: String, filename: String) -> XmlType:
    zipped = base64.b64decode(request_string)
    myzipfile = zipfile.ZipFile(io.BytesIO(zipped))

    return ET.fromstring(str(myzipfile.read(filename), "utf-8"))


@op(
    out=Out(TransactionDataFrame),
    description="import csv exportet transaction from Swiss credit card",
)
def load_csv_swiss(context, path: str, file_name: str) -> DataFrame:

    df = pd.read_csv(os.path.join(path, file_name), sep=",", decimal=".")
    rename_dict = {
        "Transaction Date": "transaction_date",
        " Posting Date": "posting_date",
        " Card Number ": "card_number",
        "Billing Amount": "billing_amount_chf",
        " Description": "merchant_name",
        " Merchant City ": "merchant_city",
        " Merchant State ": "merchant_state",
        " Merchant Zip ": "merchant_zip",
        " Reference Number ": "reference_number",
        " Debit/Credit Flag ": "debit_credit_flag",
        " SICMCC Code": "sicmcc_code",
    }
    df = df.rename(columns=rename_dict)

    # column conversion
    df["billing_amount_chf"] = (
        df["billing_amount_chf"].str.replace("[^\d.|-]", "").astype(float)
    )
    df.transaction_date = pd.to_datetime(df.transaction_date)
    df.posting_date = pd.to_datetime(df.posting_date)

    # remove none interesting columns
    df = df.drop(["debit_credit_flag", "sicmcc_code"], axis=1)
    context.log.info(f"{df.shape[0]} transactions loaded")
    context.log.info(f"dataframe columns: {df.columns}")

    return df

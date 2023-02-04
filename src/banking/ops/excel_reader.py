import os

import pandas as pd
from dagster import LocalFileHandle, OutputDefinition, String, op, solid
from dagster.utils import script_relative_path
from pandas import DataFrame, read_csv

from banking.types.pandas import BekbTransactionDataFrame, TransactionDataFrame


@op(
    output_defs=[
        OutputDefinition(
            name="bekb_transaction_dataframe", dagster_type=TransactionDataFrame
        )
    ],
    description="Reads BEKB Excel files and returns pandas dataframe",
)
def excel_reader(context, path: str, file_name: str) -> DataFrame:

    df = pd.ExcelFile(script_relative_path(os.path.join(path, file_name))).parse(0)

    rename_dict = {
        "Gutschrift / Belastung": "not needed",
        "Datum": "transaction_date",
        "Valuta": "posting_date",
        "Buchungstext": "description",
        "Zusatzinfos Buchung": "additional_info",
        "Name Auftraggeber / Begünstigter": "merchant_name",
        "Adresse Auftraggeber / Begünstigter": "merchant_address",
        "Konto / Bank": "merchant_bank",
        "Mitteilung / Referenz": "reference_number",
        "Zusatzinfos Transaktion": "additional_info_transaction",
        "Betrag": "billing_amount_chf",
        "Saldo": "saldo_chf",
    }

    df = df.rename(columns=rename_dict)
    # column conversion
    df.transaction_date = pd.to_datetime(df.transaction_date)
    df.posting_date = pd.to_datetime(df.posting_date)
    # remove none intersting columns
    df = df.drop(
        columns=[
            "not needed",
            "additional_info",
            "additional_info_transaction",
            "saldo_chf",
        ]
    )
    context.log.info(f"{df.shape[0]} transactions loaded")
    context.log.info(f"dataframe columns: {df.columns}")

    return df

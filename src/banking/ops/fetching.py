from dagster import solid, op, String, OutputDefinition, LocalFileHandle
from banking.types.pandas import BekbTransactionDataFrame
from pandas import DataFrame, read_csv
from dagster.utils import script_relative_path

import pandas as pd


@op(
    required_resource_keys={"bekb_client"},
    description="This operation will download transactions from BEKB to json",
)
def download_transactions(
    context, iban: String, language: String, date_from: String, date_to: String, copy: String,
):
    context.log.info("start connecting to BEKB and waiting for smartlogin confirmation..")

    bekb_client = context.resources.bekb_client
    session = bekb_client.smartlogin_connection()
    base64_zipped_json = bekb_client.fetch_camt053(iban, language, date_from, date_to, copy)

    context.log.info("camt053 transactions successfully donwloaded")
    bekb_client.close_connection()
    return base64_zipped_json

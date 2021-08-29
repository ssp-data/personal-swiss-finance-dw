from dagster import solid, op, String, OutputDefinition, LocalFileHandle
from banking.types.pandas import BekbTransactionDataFrame
from banking.types.general import XmlType
from pandas import DataFrame, read_csv
from dagster.utils import script_relative_path
import pandas as pd
import io
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
):
    context.log.info("start connecting to BEKB and waiting for smartlogin confirmation..")

    bekb_client = context.resources.bekb_client
    session = bekb_client.smartlogin_connection()
    json_response = bekb_client.fetch_camt053(iban, language, date_from, date_to, copy)

    context.log.info("camt053 transactions successfully donwloaded")
    bekb_client.close_connection()

    number_transaction = json_response['anzahl_auszuege']
    return decompress_and_uncompress(
        json_response['camt053_datei_inhalt'], json_response['camt053_datei_name']
    )


def decompress_and_uncompress(request_string: String, filename: String) -> XmlType:
    zipped = base64.b64decode(request_string)
    myzipfile = zipfile.ZipFile(io.BytesIO(zipped))

    return ET.fromstring(str(myzipfile.read(filename), 'utf-8'))

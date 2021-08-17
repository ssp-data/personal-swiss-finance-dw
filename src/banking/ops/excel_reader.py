from dagster import solid, op, String, OutputDefinition, LocalFileHandle
from banking.types.pandas import BekbTransactionDataFrame
from pandas import DataFrame, read_csv
from dagster.utils import script_relative_path

import pandas as pd


@op(
    output_defs=[
        OutputDefinition(name="bekb_transaction_dataframe", dagster_type=BekbTransactionDataFrame)
    ]
)
def excel_reader(context, file_path: String) -> DataFrame:
    xls = pd.ExcelFile(file_path).parse(0)
    sheet = xls.parse(0)

    df_sheet = pd.ExcelFile(script_relative_path(file_path)).parse(0)
    context.log.info("Info about read excel file: {}".format(df_sheet.info()))

    return (
        sheet,
        # parse_dates=["datum", "valuta"],
        # date_parser=lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S.%f"),
    )

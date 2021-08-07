from datetime import datetime

from dagster_pandas import PandasColumn, create_dagster_pandas_dataframe_type

BekbTransactionDataFrame = create_dagster_pandas_dataframe_type(
    name="BekbTransactionDataFrame",
    columns=[
        PandasColumn.categorical_column(
            "gutschrift_belastung", categories={"gutschrift", "belastung"}
        ),
        PandasColumn.datetime_column("datum", min_datetime=datetime(year=2000, month=1, day=1)),
        PandasColumn.datetime_column("valuta", min_datetime=datetime(year=2000, month=1, day=1)),
        PandasColumn.string_column("buchungstext"),
        PandasColumn.string_column("zusatzinfos_buchung"),
        PandasColumn.string_column("name_auftraggeber_beguenstigter"),
        PandasColumn.string_column("adresse_name_auftraggeber_beguenstigter"),
        PandasColumn.string_column("konto_bank"),
        PandasColumn.string_column("mitteilung_referenz"),
        PandasColumn.string_column("zusatzinfos_transaktion"),
        PandasColumn.float_column("Betrag"),
        PandasColumn.float_column("Saldo"),
    ],
)

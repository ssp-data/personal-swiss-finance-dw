from datetime import datetime

from dagster_pandas import PandasColumn, create_dagster_pandas_dataframe_type

BekbTransactionDataFrame = create_dagster_pandas_dataframe_type(
    name="BekbTransactionDataFrame",
    columns=[
        PandasColumn.categorical_column(
            "gutschrift_belastung", categories={"gutschrift", "belastung"}
        ),
        PandasColumn.datetime_column(
            "datum", min_datetime=datetime(year=2000, month=1, day=1)
        ),
        PandasColumn.datetime_column(
            "valuta", min_datetime=datetime(year=2000, month=1, day=1)
        ),
        PandasColumn.string_column("buchungstext"),
        PandasColumn.string_column("zusatzinfos_buchung"),
        PandasColumn.string_column("name_auftraggeber_beguenstigter"),
        PandasColumn.string_column("adresse_name_auftraggeber_beguenstigter"),
        PandasColumn.string_column("konto_bank"),
        PandasColumn.string_column("mitteilung_referenz"),
        PandasColumn.string_column("zusatzinfos_transaktion"),
        PandasColumn.float_column("Betrag"),
        PandasColumn.float_column("Saldo"),
        PandasColumn.string_column("merchant_address"),
        PandasColumn.string_column("description"),
    ],
)


def compute_transaction_dataframe_summary_statistics(dataframe):
    return {
        "min_transaction_date": min(dataframe["transaction_date"]).strftime("%Y-%m-%d"),
        "max_transaction_date": max(dataframe["transaction_date"]).strftime("%Y-%m-%d"),
        "unique merchants": str(dataframe["merchant_name"].nunique()),
        "n_rows": len(dataframe),
        "sum_billing_amount_chf": sum(dataframe["billing_amount_chf"]),
        "columns": str(dataframe.columns),
    }


TransactionDataFrame = create_dagster_pandas_dataframe_type(
    name="TransactionDataFrame",
    columns=[
        PandasColumn.datetime_column(
            "transaction_date", min_datetime=datetime(year=2000, month=1, day=1)
        ),
        PandasColumn.datetime_column("posting_date", is_required=False),
        PandasColumn.integer_column("card_number", is_required=False),
        PandasColumn.string_column("merchant_name"),  # column description
        PandasColumn.string_column("merchant_city", is_required=False),
        PandasColumn.string_column("merchant_zip", is_required=False),
        PandasColumn.string_column("merchant_state", is_required=False),
        PandasColumn.string_column("reference_number", is_required=False),
        PandasColumn.float_column("billing_amount_chf"),
        PandasColumn.float_column("billing_amount_foreign", is_required=False),
        PandasColumn.string_column(
            "billing_amount_foreign_currency", is_required=False
        ),
        PandasColumn.string_column("category", is_required=False),
        PandasColumn.string_column("notes", is_required=False),
    ],
    event_metadata_fn=compute_transaction_dataframe_summary_statistics,
)

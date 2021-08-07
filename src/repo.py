from dagster import repository
from banking.pipelines import download_pipeline


@repository
def banking_repository():
    pipelines = [
        download_pipeline,
    ]

    return pipelines

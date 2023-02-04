from dagster import (
    ConfigMapping,
    ModeDefinition,
    PresetDefinition,
    ResourceDefinition,
    file_relative_path,
    fs_io_manager,
    graph,
    mem_io_manager,
    repository,
)

from banking.ops.excel_reader import excel_reader
from banking.ops.fetching import download_transactions, load_csv_swiss
from banking.resources.resource_bekb import bekb_resource

PRESET_LOCAL = PresetDefinition(
    name="test_local_disk",
    #     run_config={
    #         "resources": dict(
    #             parquet_io_manager={"config": {"base_path": get_system_temp_directory()}},
    #             **DEFAULT_PARTITION_RESOURCE_CONFIG,
    #         ),
    #     },
    mode="test_local_data",
)


@graph()
def download_pipeline():
    #     json = download_transactions()

    load_csv_swiss()
    excel_reader()


resource_def = {
    #         "io_manager": fs_io_manager,
    #         "parquet_io_manager": partitioned_parquet_io_manager.configured(
    #             {"base_path": get_system_temp_directory()}
    #         ),
    "bekb_client": bekb_resource.configured(
        {
            "connection_url": "https://banking.bekb.ch",
            "password": os.getenv("BEKB_PASSWORD", "No bekb env set"),
            "language": "de",
            "account": os.getenv("BEKB_LOGIN", "no bekb login env set"),
            "api_version": "v3",
        },
    )
}
# download_pipeline_job = download_pipeline.to_job(resource_defs=resource_def)
download_pipeline_job = download_pipeline.to_job(
    resource_defs=resource_def,
    # config=configmapping(
    #     config_fn=lambda conf: file_relative_path(__file__, 'run_config/download_transactions.yaml')
    # )
    # config={"solids": {"do_something": {"config": {"param": "some_val"}}}}
)


@repository
def dev_repo():
    return [download_pipeline_job]

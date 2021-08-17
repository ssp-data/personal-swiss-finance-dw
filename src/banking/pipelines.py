import os

from dagster import (
    ModeDefinition,
    PresetDefinition,
    ResourceDefinition,
    fs_io_manager,
    mem_io_manager,
    graph,
)

from banking.ops.excel_reader import excel_reader

# MODE_LOCAL = ModeDefinition(
#     name="local_live_data",
#     description=(
#         "This mode queries live HN data but does all writes locally. "
#         "It is meant to be used on a local machine"
#     ),
#     resource_defs={
#         "io_manager": fs_io_manager,
#         "partition_start": ResourceDefinition.string_resource(),
#         "partition_end": ResourceDefinition.string_resource(),
#         "parquet_io_manager": partitioned_parquet_io_manager.configured(
#             {"base_path": get_system_temp_directory()}
#         ),
#         "db_io_manager": fs_io_manager,
#         "pyspark": pyspark_resource,
#         "hn_client": hn_api_subsample_client.configured({"sample_rate": 10}),
#     },
# )


# download_pipeline_properties = {
#     "description": "#### Owners:\n"
#     "hello@sspaeti.com\n "
#     "#### About\n"
#     "This pipeline .... ",
#     "mode_defs": [
#         # MODE_TEST,
#         MODE_LOCAL,
#         # MODE_STAGING,
#         # MODE_PROD,
#     ],
# }


# PRESET_TEST = PresetDefinition(
#     name="test_local_data",
#     run_config={
#         "resources": dict(
#             parquet_io_manager={"config": {"base_path": get_system_temp_directory()}},
#             **DEFAULT_PARTITION_RESOURCE_CONFIG,
#         ),
#     },
#     mode="test_local_data",
# )


# @pipeline(**download_pipeline_properties, preset_defs=[PRESET_TEST])
@graph
def download_pipeline():
    excel_reader()

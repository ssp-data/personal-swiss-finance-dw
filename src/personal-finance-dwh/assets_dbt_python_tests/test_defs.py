from assets_dbt_python import defs


def test_def_can_load():
    assert defs.get_job_def("everything_everywhere_job")

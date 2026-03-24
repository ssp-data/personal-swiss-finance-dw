.DEFAULT_GOAL := dagster

dagster:
	uv run dagster dev

install:
	uv sync --extra dev

rill:
	( \
			 cd ../rill; \
			 rill start; \
		)

run-job:
	uv run dagster job execute -j everything_everywhere_job -m assets_dbt_python

dbt-run:
	cd dbt_project && uv run dbt run --profiles-dir ./config

dbt-build:
	cd dbt_project && uv run dbt build --profiles-dir ./config

dbt-test:
	cd dbt_project && uv run dbt test --profiles-dir ./config

dbt-debug:
	cd dbt_project && uv run dbt debug --profiles-dir ./config


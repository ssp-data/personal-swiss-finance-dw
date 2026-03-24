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


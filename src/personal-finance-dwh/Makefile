.DEFAULT_GOAL := dagster

dagster:
	( \
       source ~/.venvs/banking-bekb//bin/activate; \
       dagster dev; \
    )

install:
	( \
       source ~/.venvs/banking-bekb//bin/activate; \
       pip install -e ".[dev]"; \
    )

rill:
	( \
			 cd ../rill; \
			 rill start; \
		)

run-job:
	dagster job execute -j everything_everywhere_job -m assets_dbt_python


.DEFAULT_GOAL := serve

serve:
	( \
       source ~/.venvs/banking-bekb//bin/activate; \
       dagster dev; \
    )

install:
	( \
       source ~/.venvs/banking-bekb//bin/activate; \
       pip install -e ".[dev]"; \
    )


run-job:
	dagster job execute -j everything_everywhere_job -m assets_dbt_python


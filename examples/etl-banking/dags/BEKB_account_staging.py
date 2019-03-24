# -*- coding: utf-8 -*-
#

from __future__ import print_function
import airflow
from datetime import datetime, timedelta
from acme.operators.mt940_converter_operators import write_json
from acme.operators.dwh_operators import AuditOperator
from airflow.models import Variable


args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(7),
    'provide_context': True
}

tmpl_search_path = Variable.get("sql_path")

dag = airflow.DAG(
    'bekb_account_staging',
    schedule_interval="@daily",
    dagrun_timeout=timedelta(minutes=60),
    template_searchpath=tmpl_search_path,
    default_args=args,
    max_active_runs=1)

"""get_auditid = AuditOperator(
    task_id='get_audit_id',
    postgres_conn_id='postgres_dwh',
    audit_key="customer",
    cycle_dtm="{{ ts }}",
    dag=dag,
    pool='postgres_dwh')
"""

#extract_customer = 
write_json(inputname='/ Users/sspaeti/Simon/Sync/Financials/002_Bank/2_Bekb/export/lohn_mt940.kto')

#get_auditid >> extract_customer

if __name__ == "__main__":
    dag.cli()

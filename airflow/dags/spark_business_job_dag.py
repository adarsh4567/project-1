from __future__ import annotations


import pendulum

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from utils.planner import task1

with DAG(
    dag_id="spark_business_k8s_airflow",
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["spark", "kubernetes", "kind"],
) as dag:
    
    job_config_task = PythonOperator(
        task_id="job_config_task",
        python_callable=task1
    )

    submit_spark_job = SparkKubernetesOperator(
        task_id="submit_spark_business_pi_job",
        application_file="spark-business-pi-new.yaml",
        namespace="default",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=False,
        startup_timeout_seconds=600,  
        log_events_on_failure=True,   
        reattach_on_restart=True,     
        delete_on_termination=False, 
    )

    job_config_task >> submit_spark_job

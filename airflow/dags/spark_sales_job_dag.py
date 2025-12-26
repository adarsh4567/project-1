from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator

with DAG(
    dag_id="spark_sales_k8s_airflow",
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["spark", "kubernetes", "kind"],
) as dag:
    submit_spark_job = SparkKubernetesOperator(
        task_id="submit_spark_sales_pi_job",
        application_file="spark-sales-pi.yaml",
        namespace="default",
        kubernetes_conn_id="kubernetes_default", # This will use the kubeconfig mounted in the pod
        do_xcom_push=False,
        startup_timeout_seconds=600,  # 10 minutes for pod startup
        log_events_on_failure=True,   # Log events if failure occurs
        reattach_on_restart=True,     # Reattach if scheduler restarts
        delete_on_termination=False,  # Keep pods for debugging
    )
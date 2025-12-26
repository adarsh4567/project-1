from datetime import datetime
import pytz
import boto3
import math
import yaml

BUCKET = "spark-app-storage-raw"
PARTITION_SIZE = 200
PARTITION_PER_CORE = 1
yaml_output_path = "/opt/airflow/dags/spark-business-pi-new.yaml"


s3 = boto3.client(
    's3',
    aws_access_key_id='AKIAW3MEDMWOYEEJWZY5',
    aws_secret_access_key='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',
    region_name='ap-south-1'
)

paginator = s3.get_paginator('list_objects_v2')

def next_power_of_two(n):
    """
    Returns the smallest power of 2 >= n (for n > 0).
    Handles integers efficiently via bit manipulation.
    """
    if n <= 0:
        return 1
    n -= 1
    n |= n >> 1
    n |= n >> 2
    n |= n >> 4
    n |= n >> 8
    n |= n >> 16
    n |= n >> 32  # Covers 64-bit integers
    return n + 1

def calculate_data_size():
    india_timezone = pytz.timezone('Asia/Kolkata')

    now_in_india = datetime.now(india_timezone)

    formatted_date = now_in_india.strftime('%d-%m-%Y')

    total = 0

    for page in paginator.paginate(Bucket=BUCKET, Prefix=f"raw_ecom_data/{formatted_date}"):
        for obj in page.get('Contents', []):
            total += obj['Size']

    return math.ceil(total/1000000)     


total_cores = 0
core_per_executor_instance = 0
executor_instances = 0

driver_core = 0
driver_memory = 0
    
def calculate_params(data_size:int):

    if data_size<=PARTITION_SIZE:
        return {
            "driver_core": 1,
            "driver_memory": 512,
            "executor_instances": 1,
            "core_per_executor_instance": 1,
            "memory_per_executor_instance": 512,
            "spark_maxPartitions": PARTITION_SIZE * 1024 * 1024,
            "shuffle_partitions": 1
        }
    else:
        total_cores = math.ceil(data_size/PARTITION_SIZE)

        core_per_executor_instance = min(4,total_cores)

        executor_instances = math.ceil(total_cores/core_per_executor_instance)

        calculated_memory = math.floor((PARTITION_SIZE * core_per_executor_instance * 1.2) + 1000)

        memory_per_executor_instance = next_power_of_two(calculated_memory)       

        spark_sql_maxPartitionBytes = PARTITION_SIZE * 1024 * 1024

        spark_shuffle_partitions = total_cores

        driver_core = math.ceil(min(4, total_cores / 1000))

        if driver_core == 3:
            driver_core = 4

        match driver_core:
            case 1:
                driver_memory = 512
            case 2:
                driver_memory = 1024
            case 4:
                driver_memory = 2048 

        return {
            "driver_core": driver_core,
            "driver_memory": driver_memory,
            "executor_instances": executor_instances,
            "core_per_executor_instance": core_per_executor_instance,
            "memory_per_executor_instance": memory_per_executor_instance,
            "spark_maxPartitions": spark_sql_maxPartitionBytes,
            "shuffle_partitions": spark_shuffle_partitions
        }

def render_sparkapplication_yaml(naming,config):
    sa = {
        "apiVersion": "sparkoperator.k8s.io/v1beta2",
        "kind": "SparkApplication",
        "metadata": {
            "name": naming["job_name"],
            "namespace": "default"
        },
        "spec": {
            "type": "Python",
            "pythonVersion": "3",
            "mode": "cluster",
            "image": "spark-app:2025",
            "imagePullPolicy": "IfNotPresent",
            "mainApplicationFile": naming["job_file_path"],
            "deps": {
                "jars": [
                    "local:///opt/spark/jars/hadoop-aws-3.4.1.jar",
                    "local:///opt/spark/jars/bundle-2.32.7.jar",
                    "local:///opt/spark/jars/spark-snowflake_2.13-3.1.1.jar",
                    "local:///opt/spark/jars/snowflake-jdbc-3.13.21.jar"
                ]
            },
            "sparkConf": {
                "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
                "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
                "spark.hadoop.fs.s3a.access.key": "AKIAW3MEDMWOYEEJWZY5",
                "spark.hadoop.fs.s3a.secret.key": "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",
                "spark.hadoop.fs.s3a.endpoint": "s3.ap-south-1.amazonaws.com",
                "spark.hadoop.fs.s3a.path.style.access": "true",
                "spark.driver.extraJavaOptions": "-Dcom.amazonaws.services.s3.enableV4=true",
                "spark.executor.extraJavaOptions": "-Dcom.amazonaws.services.s3.enableV4=true",
                "spark.jars.ivy": "",
                "spark.jars.packages": "",
                "spark.sql.files.maxPartitionBytes": str(config["spark_maxPartitions"]),
                "spark.sql.shuffle.partitions": str(config["shuffle_partitions"]),
                "spark.default.parallelism": str(config["shuffle_partitions"])
            },
            "arguments": ["5000"],
            "sparkVersion": "4.0.0",
            "driver": {
                "labels": {"version": "4.0.0"},
                "cores": config["driver_core"],
                "memory": f"{str(config["driver_memory"])}m",
                "serviceAccount": "spark-operator-spark",
                "securityContext": {
                    "capabilities": {"drop": ["ALL"]},
                    "runAsGroup": 185,
                    "runAsUser": 185,
                    "runAsNonRoot": True,
                    "allowPrivilegeEscalation": False,
                    "seccompProfile": {"type": "RuntimeDefault"}
                },
                "volumeMounts": [
                    {
                        "name": "business-pvc",
                        "mountPath": "/business_log"
                    }
                ]
            },
            "executor": {
                "labels": {"version": "4.0.0"},
                "instances": config["executor_instances"],
                "cores": config["core_per_executor_instance"],
                "memory": f"{config["memory_per_executor_instance"]}m",
                "securityContext": {
                    "capabilities": {"drop": ["ALL"]},
                    "runAsGroup": 185,
                    "runAsUser": 185,
                    "runAsNonRoot": True,
                    "allowPrivilegeEscalation": False,
                    "seccompProfile": {"type": "RuntimeDefault"}
                },
                "volumeMounts": [
                    {
                        "name": "business-pvc",
                        "mountPath": "/business_log"
                    }
                ]
            },
            "volumes": [
                {
                    "name": "business-pvc",
                    "persistentVolumeClaim": {
                        "claimName": "business-volume"
                    }
                }
            ]
        }
    }
    return yaml.dump(sa, default_flow_style=False, sort_keys=False)


def task1(**context):

    data_size = calculate_data_size()

    config = calculate_params(data_size=data_size)

    gen_yaml = render_sparkapplication_yaml(naming={
        "job_name":"spark-business-job",
        "job_file_path":"local:///opt/spark/app/business_job.py"
    },config=config)

    print(gen_yaml)

    with open(yaml_output_path, 'w') as f:
        f.write(gen_yaml)

    return yaml_output_path    



task1()    


    




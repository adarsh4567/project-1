FROM apache/airflow:3.0.3

USER root

COPY --chown=airflow:root ./dags/ ${AIRFLOW_HOME}/dags/

USER airflow

RUN echo "Installing package using pip.."

RUN echo "Starting.."

RUN pip install apache-airflow-providers-cncf-kubernetes

RUN pip install boto3

RUN pip install PyYAML

RUN pip install pytz

RUN echo "Installed successfully...."



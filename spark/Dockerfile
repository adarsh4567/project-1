FROM apache/spark:4.0.0

USER root

# ENV JMX_EXPORTER_AGENT_VERSION=1.1.0
# ADD https://github.com/prometheus/jmx_exporter/releases/download/${JMX_EXPORTER_AGENT_VERSION}/jmx_prometheus_javaagent-${JMX_EXPORTER_AGENT_VERSION}.jar /opt/spark/jars
# RUN chmod 644 /opt/spark/jars/jmx_prometheus_javaagent-${JMX_EXPORTER_AGENT_VERSION}.jar

# Install Python3 and pip (if not already present)
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    rm -rf /var/lib/apt/lists/* 

# (Optional) Ensure PySpark matches Spark version
RUN pip3 install pyspark

# Add Hadoop AWS and Snowflake connector jars
# COPY artifacts/*.jar /opt/spark/jars/
# RUN chown 185:185 /opt/spark/jars/*.jar && chmod 644 /opt/spark/jars/*.jar

COPY hadoop-aws-3.4.1.jar /opt/spark/jars/
COPY bundle-2.32.7.jar /opt/spark/jars/
COPY snowflake-jdbc-3.13.21.jar /opt/spark/jars/
COPY spark-snowflake_2.13-3.1.1.jar /opt/spark/jars/
COPY jmx_prometheus_javaagent-0.20.0.jar /opt/spark/jars/jmx_prometheus_javaagent-0.20.0.jar

# Fix jar permissions for Spark user
RUN chown 185:185 /opt/spark/jars/*.jar \
    && chmod 644 /opt/spark/jars/*.jar

# Create app directory and give ownership to Spark user
RUN mkdir -p /opt/spark/app && chown -R 185:185 /opt/spark/app

# Create /data for logs, owned by Spark user (not 777 world-writable)
RUN mkdir -p /data && chown 185:185 /data && chmod 775 /data
RUN mkdir -p /delivery_log && chown 185:185 /delivery_log && chmod 775 /delivery_log
RUN mkdir -p /business_log && chown 185:185 /business_log && chmod 775 /business_log
# Copy Spark applications
COPY sales_job.py /opt/spark/app/
COPY delivery_job.py /opt/spark/app/
COPY business_job.py /opt/spark/app/

WORKDIR /opt/spark/app

USER 185

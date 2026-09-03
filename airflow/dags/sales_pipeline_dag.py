"""
Daily batch pipeline DAG: Ingest -> Validate -> Transform -> Aggregate -> Load

Orchestrates the Sqoop + Spark jobs by running `docker exec` against the
already-running sqoop-client / spark-client containers (the DAG runs
inside airflow-scheduler, which has Docker socket access).
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "sonu",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "email_on_failure": False,  # no mail server configured in this env; logs serve as the alert channel
}

SPARK_ENV = (
    "export HADOOP_HOME=/opt/hadoop; "
    "export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop; "
    "export SPARK_HOME=/usr/local/lib/python3.10/dist-packages/pyspark; "
    "export PATH=$PATH:$SPARK_HOME/bin:$HADOOP_HOME/bin; "
    "export SPARK_DIST_CLASSPATH=$(hadoop classpath); "
)

SPARK_SUBMIT_BASE = (
    "spark-submit --master yarn --deploy-mode cluster "
    "--driver-memory 512m --executor-memory 512m --executor-cores 1 --num-executors 1 "
    "--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=python3 "
    "--conf spark.executorEnv.PYSPARK_PYTHON=python3 "
)

SQOOP_ENV = (
    "export HADOOP_HOME=/opt/hadoop; "
    "export HADOOP_CLASSPATH=$HADOOP_HOME/share/hadoop/common/*:$HADOOP_HOME/share/hadoop/common/lib/*:"
    "$HADOOP_HOME/share/hadoop/hdfs/*:$HADOOP_HOME/share/hadoop/hdfs/lib/*:"
    "$HADOOP_HOME/share/hadoop/mapreduce/*:$HADOOP_HOME/share/hadoop/mapreduce/lib/*:"
    "$HADOOP_HOME/share/hadoop/yarn/*:$HADOOP_HOME/share/hadoop/yarn/lib/*; "
    "export HADOOP_MAPRED_HOME=$HADOOP_HOME; "
    "export HADOOP_COMMON_HOME=$HADOOP_HOME; "
    "export HADOOP_HDFS_HOME=$HADOOP_HOME; "
    "export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop; "
    "export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop; "
)

with DAG(
    dag_id="sales_analytics_batch_pipeline",
    default_args=default_args,
    description="Daily sales analytics batch pipeline: Ingest -> Validate -> Transform -> Aggregate -> Load",
    schedule_interval="@daily",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    tags=["sales", "batch", "spark", "hive"],
) as dag:

    ingest_mysql = BashOperator(
        task_id="ingest_mysql",
        retries=5,
        retry_delay=timedelta(seconds=15),
        bash_command=(
            'docker exec sqoop-client bash -c "rm -rf /tmp/hadoop-yarn/staging/root/.staging/* /tmp/hadoop/mapred/staging/* 2>/dev/null; sleep 3; true" && '
            f'docker exec sqoop-client bash -c "{SQOOP_ENV} '
            f'/opt/sqoop/bin/sqoop import -Dfs.defaultFS=hdfs://namenode:8020 -Dmapreduce.framework.name=yarn -Dyarn.resourcemanager.address=resourcemanager:8032 -Dyarn.resourcemanager.scheduler.address=resourcemanager:8030 '
            f'--connect jdbc:mysql://mysql-operational:3306/operational '
            f'--username sqoopuser --password sqooppass '
            f'--table raw_sales --target-dir /data/raw/mysql/{{{{ ds }}}} '
            f'--delete-target-dir --num-mappers 1 --fields-terminated-by \',\' '
            f'--null-string \'\\\\\\\\N\' --null-non-string \'\\\\\\\\N\' '
            f'--columns \'order_id,customer_id,product_name,region,quantity,unit_price,order_date,created_at\'"'
        ),
    )

    ingest_csv = BashOperator(
        task_id="ingest_csv",
        bash_command=(
            'docker exec spark-client python3 /opt/spark-jobs/generate_csv_drop.py {{ ds }} '
            '&& docker exec spark-client cat /tmp/csv_drop.csv > /tmp/csv_drop_{{ ds }}.csv '
            '&& docker cp /tmp/csv_drop_{{ ds }}.csv namenode:/tmp/csv_drop_{{ ds }}.csv '
            '&& docker exec namenode hdfs dfs -mkdir -p /data/raw/csv/{{ ds }} '
            '&& docker exec namenode hdfs dfs -put -f /tmp/csv_drop_{{ ds }}.csv /data/raw/csv/{{ ds }}/sales_channel2.csv'
        ),
    )

    validate = BashOperator(
        task_id="validate",
        bash_command=(
            f'docker exec spark-client bash -c "{SPARK_ENV} '
            f'{SPARK_SUBMIT_BASE} /opt/spark-jobs/validate.py {{{{ ds }}}}"'
        ),
    )

    transform = BashOperator(
        task_id="transform",
        bash_command=(
            f'docker exec spark-client bash -c "{SPARK_ENV} '
            f'{SPARK_SUBMIT_BASE} /opt/spark-jobs/transform.py {{{{ ds }}}}"'
        ),
    )

    aggregate = BashOperator(
        task_id="aggregate",
        bash_command=(
            f'docker exec spark-client bash -c "{SPARK_ENV} '
            f'{SPARK_SUBMIT_BASE} /opt/spark-jobs/aggregate.py {{{{ ds }}}}"'
        ),
    )

    load_to_hive = BashOperator(
        task_id="load_to_hive",
        bash_command=(
            f'docker exec spark-client bash -c "{SPARK_ENV} '
            f'{SPARK_SUBMIT_BASE} /opt/spark-jobs/load_to_hive.py {{{{ ds }}}}"'
        ),
    )

    # Task dependencies: Ingest (both sources) -> Validate -> Transform -> Aggregate -> Load
    [ingest_mysql, ingest_csv] >> validate >> transform >> aggregate >> load_to_hive

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
from datetime import timedelta


with DAG(
    'bigdata_etl_dag',
    start_date=datetime(2025, 4, 18),
    schedule_interval='@daily',
    catchup=False
) as dag:
    # Step 1: Create HDFS directories if they don't exist
    create_hdfs_dirs = BashOperator(
        task_id='create_hdfs_dirs',
        bash_command='docker exec scenario3-namenode-1 hdfs dfs -mkdir -p /data/raw /data/processed'
    )
    
    # Step 2: Check if data exists in Spark container
    check_data = BashOperator(
        task_id='check_data',
        bash_command='docker exec scenario3-spark-master-1 ls -la /spark-scripts/data/'
    )
    
    # Step 3: Copy data from Spark container to NameNode for HDFS upload
    copy_to_namenode = BashOperator(
        task_id='copy_to_namenode',
        bash_command='''
        # Create temporary directory
        mkdir -p /tmp/yelp_data
        
        # Copy from Spark container to host
        docker cp scenario3-spark-master-1:/spark-scripts/data/. /tmp/yelp_data/
        
        # Copy from host to NameNode container
        docker cp /tmp/yelp_data/. scenario3-namenode-1:/tmp/yelp_data/
        '''
    )

    check_datanodes = BashOperator(
        task_id='check_datanodes',
        bash_command='docker exec scenario3-namenode-1 hdfs dfsadmin -report | grep "Live datanodes" || exit 1',
        retries=3,
        retry_delay=timedelta(minutes=5)
    )


# Step 4: Upload data to HDFS from NameNode
    upload_to_hdfs = BashOperator(
        task_id='upload_to_hdfs',
        bash_command='docker exec scenario3-namenode-1 hdfs dfs -put -f /tmp/yelp_data/* /data/raw/'
        )

    # Step 5: Process data with Spark
    ingest_task = SparkSubmitOperator(
        task_id='ingest_yelp_data',
        application='/spark-scripts/ingest_yelp_data.py',
        conn_id='spark_default',
        packages='org.apache.spark:spark-hive_2.12:3.2.0,org.postgresql:postgresql:42.7.3',
        conf={
            'spark.hadoop.fs.defaultFS': 'hdfs://scenario3-namenode-1:8020',
            'spark.hadoop.dfs.client.use.datanode.hostname': 'true',
            'spark.sql.catalogImplementation': 'hive',
            'spark.executor.memory': '2g',
            'spark.driver.memory': '2g',
            'spark.executor.cores': '2'
        },
        dag=dag,
    )

    process_task = SparkSubmitOperator(
        task_id='process_yelp_data',
        application='/spark-scripts/process_yelp_data.py',
        conn_id='spark_default',
        packages='org.apache.spark:spark-hive_2.12:3.2.0',
        conf={
            'spark.hadoop.fs.defaultFS': 'hdfs://scenario3-namenode-1:8020',
            'spark.hadoop.dfs.client.use.datanode.hostname': 'true',
            'spark.master': 'spark://spark-master:7077',
            'hive.metastore.uris': 'thrift://scenario3-hive-metastore-1:9083'
        },
        name='arrow-spark'
    )
    
    create_hdfs_dirs >> check_data >> copy_to_namenode >> check_datanodes >> upload_to_hdfs >> ingest_task >> process_task

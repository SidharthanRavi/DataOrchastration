from airflow import DAG
from airflow.providers.amazon.aws.operators.s3_file_transform import S3FileTransformOperator
from airflow.providers.scheduling.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

# Configure DAG parameters
dag_id = 'upload_folder_to_s3_v3'
local_folder = '/path/to/local/folder'
s3_bucket_name = 'your-s3-bucket-name'

# Define DAG object
with DAG(
    dag_id=dag_id,
    start_date=datetime(2024, 5, 14),
    schedule_interval=None,
) as dag:

    # Task function to upload a single file to S3
    def upload_file_to_s3(file_path, s3_bucket):
        def upload_task(ti):
            s3_key = os.path.basename(file_path)
            S3FileTransformOperator(
                task_id=f'upload_{s3_key}',
                source_path=file_path,
                dest_bucket_key=s3_key,
                dest_bucket=s3_bucket,
            ).execute(ti.context)

        return upload_task

    # Get list of files in the local folder
    with TaskGroup(group_id='get_files') as get_files:
        def get_filelist():
            file_list = [os.path.join(local_folder, f) for f in os.listdir(local_folder)]
            return file_list

        get_file_list_task = PythonOperator(
            task_id='get_file_list',
            python_callable=get_filelist,
            provide_context=True,
        )

    # Loop through each file and trigger upload task with a delay
    with TaskGroup(group_id='upload_tasks') as upload_tasks:
        for i, file in enumerate(get_files.output):
            upload_task = PythonOperator(
                task_id=f'upload_{i}',
                python_callable=upload_file_to_s3,
                op_args=[file, s3_bucket_name],
                provide_context=True,
                trigger_rule='one_success',  # Ensures tasks run sequentially
                depends_on_past=True,  # Wait for previous task before starting
            )
            # Set delay using Python's datetime with timedelta
            upload_task.set_downstream(get_file_list_task >> PythonOperator(
                task_id=f'wait_{i}',
                python_callable=lambda: datetime.now() + timedelta(minutes=10 * i),
                provide_context=True,
            ))


    # Run Glue job for data cleaning and merging
    clean_and_merge_data = GlueJobRunOperator(
        task_id='clean_and_merge_data',
        glue_job_name='your_glue_job_name',
    )

    # Trigger Lambda on new files in S3 (optional)
    trigger_lambda = LambdaInvokeOperator(
        task_id='trigger_lambda',
        function_name='your_lambda_function_name',
    )

    # Upload features to HDFS (optional, replace with your upload function)
    upload_features_to_hdfs = PythonOperator(
        task_id='upload_features_to_hdfs',
        python_callable=your_upload_features_to_hdfs_function,
        op_args={'hdfs_path': 'your_hdfs_path'},  # Pass HDFS path as argument
    )

    # Process features in PySpark and store in HDFS/Hive (optional, replace with PySpark script path)
    process_features = SparkSqlOperator(
        task_id='process_features',
        sql='your_pyspark_sql_script.py',
    )

    # Store data in RDS (optional, replace with appropriate operator and connection details)
    store_data_in_rds = MsSqlOperator(
        task_id='store_data_in_rds',
        mssql_conn_id='your_rds_connection_id',
        sql='your_sql_script_for_rds.sql',
    )

    # Push RDS data to Hive with Sqoop (optional, replace with Sqoop command)
    push_to_hive_with_sqoop = BashOperator(
        task_id='push_to_hive_with_sqoop',
        bash_command='your_sqoop_import_command',
    )
    


    # Set task dependencies
    get_files >> upload_tasks
    upload_tasks >> clean_and_merge_data
    clean_and_merge_data >> trigger_lambda  
    clean_and_merge_data >> store_data_in_rds 
    upload_features_to_hdfs >> process_features  
    process_features >> push_to_hive_with_sqoop  


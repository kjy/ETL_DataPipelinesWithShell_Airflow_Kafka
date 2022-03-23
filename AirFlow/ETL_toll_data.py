# Apache Airflow DAG that will:

# Extract data from a csv file
# Extract data from a tsv file
# Extract data from a fixed width file
# Transform the data
# Load the transformed data into the staging area


#  import the libraries

from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.operators.bash_operator import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago

#defining DAG arguments

# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'karenjyang',
    'start_date': days_ago(0),
    'email': ['karenjyang@somemail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# defining the DAG, dag_definition
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1),
)

# define the tasks

# define the first task, unzip_data

task1 = BashOperator(
    task_id='unzip_data',
    bash_command='curl https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz | tar -xz  -C /home/project/airflow/dags/finalassignment',
    #$ sudo curl http://geolite.maxmind.com/download/geoip/database/GeoLite2-Country.tar.gz | sudo tar -xz  -C /etc/nginx/
    dag=dag,
)


# define the second task, extract_data_from_csv
# Extract the columns 1 (Rowid), 2 (Timestamp), 3 (Anonymized Vehicle Number) and 
# 4(Vehicle type)
task2 = BashOperator(
    task_id='extract_data_from_csv',
    bash_command='cut -d# -f1-4 /home/project/airflow/dags/finalassignment/vehicle-data.csv > /home/project/airflow/dags/finalassignment/csv_data.csv',
    dag=dag,
)

# define the third task, extract_data_from_tsv
# Extract 5(Number of axles), 6 (Tollplaza id), 7(Tollplaza code)
task3 = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command='cut -d# -f5-7 /home/project/airflow/dags/finalassignment/tollplaza-data.tsv > /home/project/airflow/dags/finalassignment/tsv_data.csv',
    dag=dag,
)

# define the fourth task, extract_data_from_fixed_width
# 6 (Type of Payment code), 7 (Vehicle Code)
task4 = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command='cut -d# -f6-7 /home/project/airflow/dags/finalassignment/payment-data.txt > /home/project/airflow/dags/finalassignment/fixed_width_data.csv',
    dag=dag,
)

# define the fifth task, consolidate_data
task5 = BashOperator(
    task_id='consolidate_data',
    bash_command='paste /home/project/airflow/dags/finalassignment/csv_data.csv /home/project/airflow/dags/finalassignment/tsv_data.csv /home/project/airflow/dags/finalassignment/fixed_width_data.csv > /home/project/airflow/dags/finalassignment/extracted_data.csv',
    dag=dag,
)

# define the sixth task, transform_data
task6 = BashOperator(
    task_id='transform_data',
    bash_command='tr "Vehicle type" "VEHICLE TYPE"  < /home/project/airflow/dags/finalassignment/extracted_data.csv > /home/project/airflow/dags/finalassignment/staging/transformed_data.csv',
    dag=dag,
)

# task pipeline
task1 >> task2 >> task3>> task4>> task5>> task6
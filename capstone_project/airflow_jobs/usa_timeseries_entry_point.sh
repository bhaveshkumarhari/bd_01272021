source /home/hari/TAE/capstone_project/airflow_jobs/myenv/bin/activate

parallel -u ::: '/home/hari/TAE/capstone_project/airflow_jobs/usa_timeseries_kafka_producer.sh 1' '/home/hari/TAE/capstone_project/airflow_jobs/usa_timeseries_kafka_consumer.sh 2'


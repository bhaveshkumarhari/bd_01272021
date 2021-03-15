from pyspark.sql import SparkSession

spark = SparkSession\
        .builder \
        .appName("mysql_hdfs") \
        .master("local[*]") \
        .enableHiveSupport() \
        .getOrCreate()

# List for MySQL tables
mysql_tables = ['country_details','usa_details','usa_timeseries']

for table in mysql_tables:
    # This is to connect with RDBMS
    rdbms_df = spark.read \
        .format("jdbc") \
        .option('url', 'jdbc:mysql://localhost/covid19_info') \
        .option('user', 'root') \
        .option('password', 'root') \
        .option('dbtable', f"{table}") \
        .load()


    rdbms_df.createOrReplaceTempView("temp_tab")

    df=spark.sql(""" select * from temp_tab """)

    # This is to ingest data from RDBMS to HDFS as csv file
    df.write.format("csv").mode("overwrite").save(f"hdfs://localhost:9000/user/spark/input/covid19/mysql/{table}")
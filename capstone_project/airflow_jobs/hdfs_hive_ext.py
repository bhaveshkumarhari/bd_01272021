from pyspark.sql import SparkSession

spark = SparkSession\
        .builder \
        .appName("hdfs_hive_ext") \
        .master("local[*]") \
        .enableHiveSupport() \
        .getOrCreate()

# This is to create external table from HDFS location
spark.sql(""" create external table covid19_info_ext.country_details(
                            id int,country string,country_code string,confirmed_cases int,today_cases int,deaths int,
                            today_deaths int,recovered int,today_recovered int,critical int,total_tests int)
                            ROW FORMAT DELIMITED
                            FIELDS TERMINATED BY ','
                            location '/user/spark/input/covid19/mysql/country_details' """)

spark.sql(""" create external table covid19_info_ext.usa_details(
                            id int,state string,confirmed_cases int,today_cases int,
                            deaths int,today_deaths int,recovered int,total_tests int)
                            ROW FORMAT DELIMITED
                            FIELDS TERMINATED BY ','
                            location '/user/spark/input/covid19/mysql/usa_details' """)

spark.sql(""" create external table covid19_info_ext.usa_timeseries(
                            id int,date date,confirmed_cases int,deaths int,recovered int)
                            ROW FORMAT DELIMITED
                            FIELDS TERMINATED BY ','
                            location '/user/spark/input/covid19/mysql/usa_timeseries' """)
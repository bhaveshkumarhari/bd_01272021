from pyspark.sql import SparkSession

spark = SparkSession\
        .builder \
        .appName("hive_ext_int") \
        .master("local[*]") \
        .enableHiveSupport() \
        .getOrCreate()


# FOR EACH COUNTRY
#--------------------
#Confirmed Cases with RANK
#--------------------

country_cases_rank_df=spark.sql(""" SELECT * FROM covid19_info_int.country_cases_rank """)

country_cases_rank_df.select("id","country","country_code","confirmed_cases","rank").write.format("jdbc") \
  .mode("overwrite") \
  .option("url", 'jdbc:postgresql://localhost/covid19_info') \
  .option("dbtable", "country_cases_rank") \
  .option("user", 'postgres') \
  .option("password", 'root') \
  .save()

#--------------------
#Confirmed Cases
#--------------------

country_cases_df=spark.sql(""" SELECT * FROM covid19_info_int.country_cases """)

country_cases_df.select("id","country","confirmed_cases").write.format("jdbc") \
  .mode("overwrite") \
  .option("url", 'jdbc:postgresql://localhost/covid19_info') \
  .option("dbtable", "country_cases") \
  .option("user", 'postgres') \
  .option("password", 'root') \
  .save()

#--------------------
#Deaths
#--------------------

country_deaths_df=spark.sql(""" SELECT * FROM covid19_info_int.country_deaths """)

country_deaths_df.select("id","country","deaths").write.format("jdbc") \
  .mode("overwrite") \
  .option("url", 'jdbc:postgresql://localhost/covid19_info') \
  .option("dbtable", "country_deaths") \
  .option("user", 'postgres') \
  .option("password", 'root') \
  .save()

#--------------------
#Recovered
#--------------------

country_recovered_df=spark.sql(""" SELECT * FROM covid19_info_int.country_recovered """)

country_recovered_df.select("id","country","recovered").write.format("jdbc") \
  .mode("overwrite") \
  .option("url", 'jdbc:postgresql://localhost/covid19_info') \
  .option("dbtable", "country_recovered") \
  .option("user", 'postgres') \
  .option("password", 'root') \
  .save()

#--------------------
#Critical
#--------------------

country_critical_df=spark.sql(""" SELECT * FROM covid19_info_int.country_critical """)

country_critical_df.select("id","country","critical").write.format("jdbc") \
  .mode("overwrite") \
  .option("url", 'jdbc:postgresql://localhost/covid19_info') \
  .option("dbtable", "country_critical") \
  .option("user", 'postgres') \
  .option("password", 'root') \
  .save()

#--------------------
#Total Tests
#--------------------

country_tests_df=spark.sql(""" SELECT * FROM covid19_info_int.country_tests """)

country_tests_df.select("id","country","total_tests").write.format("jdbc") \
  .mode("overwrite") \
  .option("url", 'jdbc:postgresql://localhost/covid19_info') \
  .option("dbtable", "country_tests") \
  .option("user", 'postgres') \
  .option("password", 'root') \
  .save()

#FOR EACH STATE OF USA
#--------------------
#Confirmed Cases
#--------------------

state_cases_df=spark.sql(""" SELECT * FROM covid19_info_int.state_cases """)

state_cases_df.select("id","state","confirmed_cases").write.format("jdbc") \
  .mode("overwrite") \
  .option("url", 'jdbc:postgresql://localhost/covid19_info') \
  .option("dbtable", "state_cases") \
  .option("user", 'postgres') \
  .option("password", 'root') \
  .save()

#--------------------
#Deaths
#--------------------

state_deaths_df=spark.sql(""" SELECT * FROM covid19_info_int.state_deaths """)

state_deaths_df.select("id","state","deaths").write.format("jdbc") \
  .mode("overwrite") \
  .option("url", 'jdbc:postgresql://localhost/covid19_info') \
  .option("dbtable", "state_deaths") \
  .option("user", 'postgres') \
  .option("password", 'root') \
  .save()

#--------------------
#Recovered
#--------------------

state_recovered_df=spark.sql(""" SELECT * FROM covid19_info_int.state_recovered """)

state_recovered_df.select("id","state","recovered").write.format("jdbc") \
  .mode("overwrite") \
  .option("url", 'jdbc:postgresql://localhost/covid19_info') \
  .option("dbtable", "state_recovered") \
  .option("user", 'postgres') \
  .option("password", 'root') \
  .save()

#--------------------
#Total Tests
#--------------------

state_tests_df=spark.sql(""" SELECT * FROM covid19_info_int.state_tests """)

state_tests_df.select("id","state","total_tests").write.format("jdbc") \
  .mode("overwrite") \
  .option("url", 'jdbc:postgresql://localhost/covid19_info') \
  .option("dbtable", "state_tests") \
  .option("user", 'postgres') \
  .option("password", 'root') \
  .save()


#TIMESERIES FOR USA STATES

usa_timeseries_begin_df=spark.sql(""" SELECT * FROM covid19_info_int.usa_timeseries_begin """)

usa_timeseries_begin_df.select("id","date","confirmed_cases","deaths","recovered").write.format("jdbc") \
  .mode("overwrite") \
  .option("url", 'jdbc:postgresql://localhost/covid19_info') \
  .option("dbtable", "usa_timeseries_begin") \
  .option("user", 'postgres') \
  .option("password", 'root') \
  .save()

usa_timeseries_end_df=spark.sql(""" SELECT * FROM covid19_info_int.usa_timeseries_end """)

usa_timeseries_end_df.select("id","date","confirmed_cases","deaths","recovered").write.format("jdbc") \
  .mode("overwrite") \
  .option("url", 'jdbc:postgresql://localhost/covid19_info') \
  .option("dbtable", "usa_timeseries_end") \
  .option("user", 'postgres') \
  .option("password", 'root') \
  .save()


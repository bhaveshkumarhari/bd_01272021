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

# This is for aggregation from external table
country_cases_rank_df=spark.sql(""" SELECT DISTINCT id, country, country_code, confirmed_cases,
                            ROW_NUMBER() OVER (ORDER BY confirmed_cases DESC) AS rank
                            FROM covid19_info_ext.country_details order by confirmed_cases DESC""")
#country_cases_rank_df.show()
# This is for creating internal table with aggregation
country_cases_rank_df.write.format("orc").mode("overwrite").saveAsTable("covid19_info_int.country_cases_rank")

#--------------------
#Confirmed Cases
#--------------------

# This is for aggregation from external table
country_cases_df=spark.sql(""" SELECT DISTINCT id, country, confirmed_cases FROM covid19_info_ext.country_details order by confirmed_cases DESC LIMIT 10""")
#country_cases_df.show()
# This is for creating internal table with aggregation
country_cases_df.write.format("orc").mode("overwrite").saveAsTable("covid19_info_int.country_cases")


#--------------------
#Deaths
#--------------------

# This is for aggregation from external table
country_deaths_df=spark.sql(""" SELECT DISTINCT id, country, deaths FROM covid19_info_ext.country_details order by deaths DESC LIMIT 10""")
#country_deaths_df.show()
# This is for creating internal table with aggregation
country_deaths_df.write.format("orc").mode("overwrite").saveAsTable("covid19_info_int.country_deaths")

#--------------------
#Recovered
#--------------------

# This is for aggregation from external table
country_recovered_df=spark.sql(""" SELECT DISTINCT id, country, recovered FROM covid19_info_ext.country_details order by recovered DESC LIMIT 10""")
#country_recovered_df.show()
# This is for creating internal table with aggregation
country_recovered_df.write.format("orc").mode("overwrite").saveAsTable("covid19_info_int.country_recovered")

#--------------------
#Critical
#--------------------

# This is for aggregation from external table
country_critical_df=spark.sql(""" SELECT DISTINCT id, country, critical FROM covid19_info_ext.country_details order by critical DESC LIMIT 10""")
#country_critical_df.show()
# This is for creating internal table with aggregation
country_critical_df.write.format("orc").mode("overwrite").saveAsTable("covid19_info_int.country_critical")

#--------------------
#Total Tests
#--------------------

# This is for aggregation from external table
country_tests_df=spark.sql(""" SELECT DISTINCT id, country, total_tests FROM covid19_info_ext.country_details order by total_tests DESC LIMIT 10""")
#country_tests_df.show()
# This is for creating internal table with aggregation
country_tests_df.write.format("orc").mode("overwrite").saveAsTable("covid19_info_int.country_tests")

# FOR EACH STATE OF USA
#--------------------
#Confirmed Cases
#--------------------

# This is for aggregation from external table
state_cases_df=spark.sql(""" SELECT DISTINCT id, state, confirmed_cases FROM covid19_info_ext.usa_details order by confirmed_cases DESC LIMIT 10""")
#state_cases_df.show()
# This is for creating internal table with aggregation
state_cases_df.write.format("orc").mode("overwrite").saveAsTable("covid19_info_int.state_cases")

#--------------------
#Deaths
#--------------------

# This is for aggregation from external table
state_deaths_df=spark.sql(""" SELECT DISTINCT id, state, deaths FROM covid19_info_ext.usa_details order by deaths DESC LIMIT 10""")
#state_deaths_df.show()
# This is for creating internal table with aggregation
state_deaths_df.write.format("orc").mode("overwrite").saveAsTable("covid19_info_int.state_deaths")

#--------------------
#Recovered
#--------------------

# This is for aggregation from external table
state_recovered_df=spark.sql(""" SELECT DISTINCT id, state, recovered FROM covid19_info_ext.usa_details order by recovered DESC LIMIT 10""")
#state_recovered_df.show()
# This is for creating internal table with aggregation
state_recovered_df.write.format("orc").mode("overwrite").saveAsTable("covid19_info_int.state_recovered")

#--------------------
#Total Tests
#--------------------

# This is for aggregation from external table
state_tests_df=spark.sql(""" SELECT DISTINCT id, state, total_tests FROM covid19_info_ext.usa_details order by total_tests DESC LIMIT 10""")
#state_tests_df.show()
# This is for creating internal table with aggregation
state_tests_df.write.format("orc").mode("overwrite").saveAsTable("covid19_info_int.state_tests")

#TIMESERIES FOR USA STATES

# This is for aggregation from external table
state_timeseries_begin_df=spark.sql(""" SELECT DISTINCT * FROM covid19_info_ext.usa_timeseries order by date LIMIT 150 """)
#state_timeseries_begin_df.show()
# This is for creating internal table with aggregation
state_timeseries_begin_df.write.format("orc").mode("overwrite").saveAsTable("covid19_info_int.usa_timeseries_begin")

# This is for aggregation from external table
state_timeseries_end_df=spark.sql(""" SELECT DISTINCT * FROM (
                                    SELECT * FROM covid19_info_ext.usa_timeseries order by date DESC LIMIT 150)
                                    ORDER BY date ASC """)
#state_timeseries_end_df.show()
# This is for creating internal table with aggregation
state_timeseries_end_df.write.format("orc").mode("overwrite").saveAsTable("covid19_info_int.usa_timeseries_end")

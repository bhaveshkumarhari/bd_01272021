from pyspark.sql import SparkSession

spark = SparkSession\
        .builder \
        .appName("rdbms_hive") \
        .master("local[*]") \
        .enableHiveSupport() \
        .getOrCreate()

# spark.set("spark.jars","/home/hadoop/spark/jars/postgresql-42.2.11.jar")


# df = spark.read \
#     .format("jdbc") \
#     .option("url", "jdbc:postgresql://localhost:5432/databasename") \
#     .option("dbtable", "tablename") \
#     .option("user", "username") \
#     .option("password", "password") \
#     .option("driver", "org.postgresql.Driver") \
#     .load()
#
# df.printSchema()

#----------------------------
#MySQL
#----------------------------

# List for MySQL tables
mysql_tables = ['customers','contacts','orders','employees']

for table in mysql_tables:
    # This is to connect with RDBMS
    rdbms_df = spark.read \
        .format("jdbc") \
        .option('url', 'jdbc:mysql://localhost/store') \
        .option('user', 'root') \
        .option('password', 'root') \
        .option('dbtable', f"{table}") \
        .load()


    rdbms_df.createOrReplaceTempView("temp_tab")

    df=spark.sql(""" select * from temp_tab """)

    # This is to ingest data from RDBMS to HDFS as csv file
    df.write.format("csv").mode("overwrite").save(f"hdfs://localhost:9000/user/spark/input/store/mysql/{table}")


#----------------------------
#MySQL - Customers
#----------------------------

# This is to create external table from HDFS location
spark.sql(""" create external table store.customers_ext(customer_id int,name string,address string,website string,credit_limit int)
                            ROW FORMAT DELIMITED
                            FIELDS TERMINATED BY ','
                            location '/user/spark/input/store/mysql/customers' """)

# This is for aggregation from external table
agg_customers_df=spark.sql(""" SELECT * FROM store.customers_ext WHERE credit_limit < 200 """)

# This is for creating internal table with aggregation
agg_customers_df.write.format("orc").mode("overwrite").saveAsTable("store.customers_int")

# Send Aggregated data back to RDBMS
agg_customers_df.select("customer_id","name", "address", "website", "credit_limit").write.format("jdbc") \
  .mode("overwrite") \
  .option("url", 'jdbc:mysql://localhost/store_output') \
  .option("dbtable", "customers") \
  .option("user", 'root') \
  .option("password", 'root') \
  .save()


#----------------------------
#MySQL - Contacts
#----------------------------


spark.sql(""" create external table store.contacts_ext(contact_id int,first_name string,last_name string,email string,phone string,customer_id int)
                            ROW FORMAT DELIMITED
                            FIELDS TERMINATED BY ','
                            location '/user/spark/input/store/mysql/contacts' """)

agg_contacts_df=spark.sql(""" SELECT * FROM store.contacts_ext """)

agg_contacts_df.write.format("orc").mode("overwrite").saveAsTable("store.contacts_int")

agg_contacts_df.select("contact_id","first_name", "last_name", "email", "phone", "customer_id").write.format("jdbc") \
  .mode("overwrite") \
  .option("url", 'jdbc:mysql://localhost/store_output') \
  .option("dbtable", "contacts") \
  .option("user", 'root') \
  .option("password", 'root') \
  .save()


#----------------------------
#MySQL - Orders
#----------------------------

spark.sql(""" create external table store.orders_ext(order_id int,customer_id int,status string,salesman_id int,order_date date)
                            ROW FORMAT DELIMITED
                            FIELDS TERMINATED BY ','
                            location '/user/spark/input/store/mysql/orders' """)

agg_orders_df=spark.sql(""" SELECT * FROM store.orders_ext WHERE status='Shipped' """)

agg_orders_df.write.format("orc").mode("overwrite").saveAsTable("store.orders_int")

agg_orders_df.select("order_id","customer_id", "status", "salesman_id", "order_date").write.format("jdbc") \
  .mode("overwrite") \
  .option("url", 'jdbc:mysql://localhost/store_output') \
  .option("dbtable", "orders") \
  .option("user", 'root') \
  .option("password", 'root') \
  .save()


#----------------------------
#MySQL - Employees
#----------------------------
spark.sql(""" create external table store.employees_ext(employee_id int,first_name string,last_name string,email string,phone string,hire_date date,manager_id int,job_title string)
                            ROW FORMAT DELIMITED
                            FIELDS TERMINATED BY ','
                            location '/user/spark/input/store/mysql/employees' """)

agg_employees_df=spark.sql(""" SELECT employee_id, first_name, last_name, email, phone, hire_date, manager_id, job_title, ROW_NUMBER() OVER (ORDER BY hire_date) AS row_rank FROM store.employees_ext ORDER BY row_rank """)

agg_employees_df.write.format("orc").mode("overwrite").saveAsTable("store.employees_int")

agg_employees_df.select("employee_id","first_name", "last_name", "email", "phone", "hire_date", "manager_id", "job_title", "row_rank").write.format("jdbc") \
  .mode("overwrite") \
  .option("url", 'jdbc:mysql://localhost/store_output') \
  .option("dbtable", "employees") \
  .option("user", 'root') \
  .option("password", 'root') \
  .save()

#----------------------------
#PostgreSQL
#----------------------------

postgres_tables = ['order_items','products','product_categories','inventories']

for table in postgres_tables:
    rdbms_df = spark.read \
        .format("jdbc") \
        .option('url', 'jdbc:postgresql://localhost/store') \
        .option('user', 'postgres') \
        .option('password', 'root') \
        .option('dbtable', f"{table}") \
        .load()


    rdbms_df.createOrReplaceTempView("temp_tab")

    df=spark.sql(""" select * from temp_tab """)

    df.write.format("csv").mode("overwrite").save(f"hdfs://localhost:9000/user/spark/input/store/postgres/{table}")

#----------------------------
#PostgreSQL - Order Items
#----------------------------

spark.sql(""" create external table store.order_items_ext(order_id int,item_id int,product_id int,quantity int,unit_price int)
                            ROW FORMAT DELIMITED
                            FIELDS TERMINATED BY ','
                            location '/user/spark/input/store/postgres/order_items' """)

agg_order_items_df=spark.sql(""" SELECT order_id, item_id, product_id, quantity, unit_price, RANK() OVER (ORDER BY unit_price) AS row_rank FROM store.order_items_ext ORDER BY row_rank """)

agg_order_items_df.write.format("orc").mode("overwrite").saveAsTable("store.order_items_int")

agg_order_items_df.select("order_id","item_id", "product_id", "quantity", "unit_price", "row_rank").write.format("jdbc") \
  .mode("overwrite") \
  .option("url", 'jdbc:postgresql://localhost/store_output') \
  .option("dbtable", "order_items") \
  .option("user", 'postgres') \
  .option("password", 'root') \
  .save()


#----------------------------
#PostgreSQL - Products
#----------------------------

spark.sql(""" create external table store.products_ext(product_id int,product_name string,description string,standard_cost int,list_price int,category_id int)
                            ROW FORMAT DELIMITED
                            FIELDS TERMINATED BY ','
                            location '/user/spark/input/store/postgres/products' """)

agg_products_df=spark.sql(""" SELECT product_id, product_name, description, standard_cost, list_price, category_id, DENSE_RANK() OVER (ORDER BY standard_cost) AS row_rank FROM store.products_ext """)

agg_products_df.write.format("orc").mode("overwrite").saveAsTable("store.products_int")

agg_products_df.select("product_id","product_name", "description", "standard_cost", "list_price", "category_id", "row_rank").write.format("jdbc") \
  .mode("overwrite") \
  .option("url", 'jdbc:postgresql://localhost/store_output') \
  .option("dbtable", "products") \
  .option("user", 'postgres') \
  .option("password", 'root') \
  .save()


#----------------------------
#PostgreSQL - Product Categories
#----------------------------

spark.sql(""" create external table store.product_categories_ext(category_id int,category_name string)
                            ROW FORMAT DELIMITED
                            FIELDS TERMINATED BY ','
                            location '/user/spark/input/store/postgres/product_categories' """)

agg_product_categories_df=spark.sql(""" SELECT * FROM store.product_categories_ext """)

agg_product_categories_df.write.format("orc").mode("overwrite").saveAsTable("store.product_categories_int")

agg_product_categories_df.select("category_id","category_name").write.format("jdbc") \
  .mode("overwrite") \
  .option("url", 'jdbc:postgresql://localhost/store_output') \
  .option("dbtable", "product_categories") \
  .option("user", 'postgres') \
  .option("password", 'root') \
  .save()

#----------------------------
#PostgreSQL - Inventories
#----------------------------

spark.sql(""" create external table store.inventories_ext(product_id int,warehouse_id int,quantity int)
                            ROW FORMAT DELIMITED
                            FIELDS TERMINATED BY ','
                            location '/user/spark/input/store/postgres/inventories' """)

agg_inventories_df=spark.sql(""" SELECT * FROM store.inventories_ext """)

agg_inventories_df.write.format("orc").mode("overwrite").saveAsTable("store.inventories_int")

agg_inventories_df.select("product_id","warehouse_id","quantity").write.format("jdbc") \
  .mode("overwrite") \
  .option("url", 'jdbc:postgresql://localhost/store_output') \
  .option("dbtable", "inventories") \
  .option("user", 'postgres') \
  .option("password", 'root') \
  .save()

#----------------------------
#Oracle
#----------------------------

oracle_tables = ['warehouses','locations']

for table in oracle_tables:
    rdbms_df = spark.read \
        .format("jdbc") \
        .option('url', 'jdbc:oracle:thin:@//localhost/xe') \
        .option('user', 'store') \
        .option('password', 'root') \
        .option('dbtable', f"{table}") \
        .load()


    rdbms_df.createOrReplaceTempView("temp_tab")

    df=spark.sql(""" select * from temp_tab """)

    df.write.format("csv").mode("overwrite").save(f"hdfs://localhost:9000/user/spark/input/store/oracle/{table}")

#----------------------------
#Oracle - Warehouses
#----------------------------

spark.sql(""" create external table store.warehouses_ext(warehouse_id int,warehouse_name string,location_id int)
                            ROW FORMAT DELIMITED
                            FIELDS TERMINATED BY ','
                            location '/user/spark/input/store/oracle/warehouses' """)

agg_warehouses_df=spark.sql(""" SELECT * FROM store.warehouses_ext """)

agg_warehouses_df.write.format("orc").mode("overwrite").saveAsTable("store.warehouses_int")

agg_warehouses_df.select("warehouse_id","warehouse_name","location_id").write.format("jdbc") \
  .mode("overwrite") \
  .option("url", 'jdbc:oracle:thin:@//localhost/xe') \
  .option("dbtable", "warehouses") \
  .option("user", 'store_output') \
  .option("password", 'root') \
  .save()


#----------------------------
#Oracle - Locations
#----------------------------

spark.sql(""" create external table store.locations_ext(location_id int,address string,postal_code string,city string,state string,country string)
                            ROW FORMAT DELIMITED
                            FIELDS TERMINATED BY ','
                            location '/user/spark/input/store/oracle/locations' """)

agg_locations_df=spark.sql(""" SELECT * FROM store.locations_ext """)

agg_locations_df.write.format("orc").mode("overwrite").saveAsTable("store.locations_int")

agg_locations_df.select("location_id","address","postal_code","city","state","country").write.format("jdbc") \
  .mode("overwrite") \
  .option("url", 'jdbc:oracle:thin:@//localhost/xe') \
  .option("dbtable", "locations") \
  .option("user", 'store_output') \
  .option("password", 'root') \
  .save()

use store;

create table customers_hbase(customer_id int,customer_name string,customer_address string,customer_website string,credit_limit int)
stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
with serdeproperties("hbase.columns.mapping"=":key,x:customer_name,x:customer_address,x:customer_website,x:credit_limit");

INSERT OVERWRITE TABLE customers_hbase SELECT * FROM customers WHERE credit_limit < 200;


create table contacts_hbase(contact_id int,first_name string,last_name string,email string,phone string,customer_id int)
stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
with serdeproperties("hbase.columns.mapping"=":key,x:first_name,x:last_name,x:email,x:phone,x:customer_id");

INSERT OVERWRITE TABLE contacts_hbase SELECT * FROM contacts;


create table orders_hbase(order_id int,customer_id int,status string,salesman_id int,order_date date)
stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
with serdeproperties("hbase.columns.mapping"=":key,x:customer_id,x:status,x:salesman_id,x:order_date");

INSERT OVERWRITE TABLE orders_hbase SELECT * FROM orders WHERE status='Shipped';


create table employees_hbase(employee_id int,first_name string,last_name string,email string,phone string,hire_date date,manager_id int,job_title string,row_rank int)
stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
with serdeproperties("hbase.columns.mapping"=":key,x:first_name,x:last_name,x:email,x:phone,x:hire_date,x:manager_id,x:job_title,x:row_rank");

INSERT OVERWRITE TABLE employees_hbase SELECT employee_id, first_name, last_name, email, phone, hire_date, manager_id, job_title, ROW_NUMBER() OVER (ORDER BY hire_date) AS row_rank FROM employees ORDER BY row_rank;


create table order_items_hbase(order_id int,item_id int,product_id int,quantity int,unit_price int,row_rank int)
stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
with serdeproperties("hbase.columns.mapping"=":key,x:item_id,x:product_id,x:quantity,x:unit_price,x:row_rank");

INSERT OVERWRITE TABLE order_items_hbase SELECT order_id, item_id, product_id, quantity, unit_price, RANK() OVER (ORDER BY unit_price) AS row_rank FROM order_items ORDER BY row_rank;


create table products_hbase(product_id int,product_name string,description string,standard_cost int,list_price int,category_id int,row_num int)
stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
with serdeproperties("hbase.columns.mapping"=":key,x:product_name,x:description,x:standard_cost,x:list_price,x:category_id,x:row_num");

INSERT OVERWRITE TABLE products_hbase SELECT product_id, product_name, description, standard_cost, list_price, category_id, DENSE_RANK() OVER (ORDER BY standard_cost) AS row_rank FROM products;


create table product_categories_hbase(category_id int,category_name string)
stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
with serdeproperties("hbase.columns.mapping"=":key,x:category_name");

INSERT OVERWRITE TABLE product_categories_hbase SELECT * FROM product_categories;


create table inventories_hbase(product_id int,warehouse_id int,quantity int)
stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
with serdeproperties("hbase.columns.mapping"=":key,x:warehouse_id,x:quantity");

INSERT OVERWRITE TABLE inventories_hbase SELECT * FROM inventories;


create table warehouses_hbase(warehouse_id int,warehouse_name string,location_id int)
stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
with serdeproperties("hbase.columns.mapping"=":key,x:warehouse_name,x:location_id");

INSERT OVERWRITE TABLE warehouses_hbase SELECT * FROM warehouses;


create table locations_hbase(location_id int,address string,postal_code string,city string,state string,country string)
stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
with serdeproperties("hbase.columns.mapping"=":key,x:address,x:postal_code,x:city,x:state,x:country");

INSERT OVERWRITE TABLE locations_hbase SELECT * FROM locations;

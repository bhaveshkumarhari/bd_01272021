create database store;

use store;

create external table customers(customer_id int,customer_name string,customer_address string,customer_website string,credit_limit int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
location '/user/hadoop/input/store/mysql/customers';

create external table contacts(contact_id int,first_name string,last_name string,email string,phone string,customer_id int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
location '/user/hadoop/input/store/mysql/contacts';


create external table orders(order_id int,customer_id int,status string,salesman_id int,order_date date)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
location '/user/hadoop/input/store/mysql/orders';


create external table employees(employee_id int,first_name string,last_name string,email string,phone string,hire_date date,manager_id int,job_title string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
location '/user/hadoop/input/store/mysql/employees';


create external table order_items(order_id int,item_id int,product_id int,quantity int,unit_price int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
location '/user/hadoop/input/store/postgres/order_items';


create external table products(product_id int,product_name string,description string,standard_cost int,list_price int,category_id int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
location '/user/hadoop/input/store/postgres/products';


create external table product_categories(category_id int,category_name string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
location '/user/hadoop/input/store/postgres/product_categories';

create external table inventories(product_id int,warehouse_id int,quantity int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
location '/user/hadoop/input/store/postgres/inventories';


create external table warehouses(warehouse_id int,warehouse_name string,location_id int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
location '/user/hadoop/input/store/oracle/warehouses';


create external table locations(location_id int,address string,postal_code string,city string,state string,country string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
location '/user/hadoop/input/store/oracle/locations';

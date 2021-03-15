from kafka import KafkaConsumer
from json import loads

import mysql.connector

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="root",
  database="covid19_info"
)

mycursor = mydb.cursor()


KAFKA_CONSUMER_GROUP_NAME_CONS = "consumer_group"
KAFKA_TOPIC_NAME_CONS = "covid19usa"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'

if __name__ == "__main__":

    print("Kafka Consumer Application Started ... ")
    # auto_offset_reset='latest'
    # auto_offset_reset='earliest'
    consumer = KafkaConsumer(
    KAFKA_TOPIC_NAME_CONS,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS,
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id=KAFKA_CONSUMER_GROUP_NAME_CONS,
    consumer_timeout_ms=60000,
    value_deserializer=lambda x: loads(x.decode('utf-8')))

    for message in consumer:
        message = message.value
        print(message)
        columns = ', '.join("`" + str(x).replace('/', '_') + "`" for x in message.keys())
        values = ', '.join("'" + str(x).replace('/', '_') + "'" for x in message.values())
        sql = "INSERT INTO %s ( %s ) VALUES ( %s );" % ('usa_details', columns, values)
        mycursor.execute(sql)
        mydb.commit()
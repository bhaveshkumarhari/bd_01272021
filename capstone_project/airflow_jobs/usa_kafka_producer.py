from kafka import KafkaProducer
import time
from json import dumps

import requests
import json

KAFKA_TOPIC_NAME_CONS = "covid19usa"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'

class CovidAPI:
    def __init__(self):
        self.api_url = "https://corona.lmao.ninja/v2/states"

    def get_data(self):
        response = requests.get(self.api_url)
        raw_data = json.loads(response.text)
        return raw_data

if __name__ == "__main__":
    print("Kafka Producer Application Started ... ")

    kafka_producer_obj = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS,
                             value_serializer=lambda x: dumps(x).encode('utf-8'))

    cls_object = CovidAPI()
    get_data = cls_object.get_data()
    #print(get_data)

    dict_data = None
    i = 0
    for data in get_data:
        i = i + 1
        dict_data = {}
        print("Sending message to Kafka topic: " + str(i))
        #print(data["country"].replace("'", " "))
        dict_data["id"] = i
        dict_data["state"] = data["state"].replace("'", " ")
        dict_data["confirmed_cases"] = data["cases"]
        dict_data["today_cases"] = data["todayCases"]
        dict_data["deaths"] = data["deaths"]
        dict_data["today_deaths"] = data["todayDeaths"]
        dict_data["recovered"] = data["recovered"]
        dict_data["total_tests"] = data["tests"]

        print("Message to be sent: ", dict_data)
        kafka_producer_obj.send(KAFKA_TOPIC_NAME_CONS, dict_data)
        time.sleep(0.1)
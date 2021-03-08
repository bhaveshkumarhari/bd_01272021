from kafka import KafkaProducer
from datetime import datetime
import time
from json import dumps
import random
import requests
import json

KAFKA_TOPIC_NAME_CONS = "covid19topic"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'

class CovidAPI:
    def __init__(self):
        self.api_url = "https://corona.lmao.ninja/v2/countries"

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
        dict_data["Country"] = data["country"]
        dict_data["Cases"] = data["cases"]
        dict_data["Today's Cases"] = data["todayCases"]
        dict_data["Deaths"] = data["deaths"]
        dict_data["Today's Deaths"] = data["todayDeaths"]
        dict_data["Recovered"] = data["recovered"]
        dict_data["Today's Recovered"] = data["todayRecovered"]
        dict_data["Critical"] = data["critical"]
        dict_data["Total Tests"] = data["tests"]

        print("Message to be sent: ", dict_data)
        kafka_producer_obj.send(KAFKA_TOPIC_NAME_CONS, dict_data)
        time.sleep(1)
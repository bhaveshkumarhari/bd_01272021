from kafka import KafkaProducer
import time
from json import dumps

import requests
import json

KAFKA_TOPIC_NAME_CONS = "covid19usatimeseries"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'

class CovidAPI:
    def __init__(self):
        self.api_url = "https://pomber.github.io/covid19/timeseries.json"

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
    for key, value in get_data.items():
        if key == "US":
            counts = len(value)
            for count in range(counts):
                i = i + 1
                dict_data = {}
                print("Sending message to Kafka topic: " + str(i))
                dict_data["id"] = i
                dict_data["date"] = value[count]['date']
                dict_data["confirmed_cases"] = value[count]['confirmed']
                dict_data["deaths"] = value[count]['deaths']
                dict_data["recovered"] = value[count]['recovered']

                print("Message to be sent: ", dict_data)
                kafka_producer_obj.send(KAFKA_TOPIC_NAME_CONS, dict_data)
                time.sleep(0.1)



    # for data in get_data:
    #     i = i + 1
    #     dict_data = {}
    #     print("Sending message to Kafka topic: " + str(i))
    #     #print(data["country"].replace("'", " "))
    #     dict_data["country"] = data["state"].replace("'", " ")
    #     dict_data["confirmed_cases"] = data["cases"]
    #     dict_data["today_cases"] = data["todayCases"]
    #     dict_data["deaths"] = data["deaths"]
    #     dict_data["today_deaths"] = data["todayDeaths"]
    #     dict_data["recovered"] = data["recovered"]
    #     dict_data["total_tests"] = data["tests"]
    #
    #     print("Message to be sent: ", dict_data)
    #     kafka_producer_obj.send(KAFKA_TOPIC_NAME_CONS, dict_data)
    #     time.sleep(0.5)
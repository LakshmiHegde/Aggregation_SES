import json
import time
from kafka import KafkaProducer

from BI_SIGNAL.Data_Generation_BSES import DataGeneration_BSES


def json_serializer(data):
    return json.dumps(data).encode("utf-8")  # mess is serialized in json format


producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=json_serializer,
                         )
dg = DataGeneration_BSES()
while 1 == 1:
    ad = dg.get_event()
    print(ad)
    producer.send("BSES", ad)
    time.sleep(2)

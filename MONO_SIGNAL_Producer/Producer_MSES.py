import json
import time
from kafka import KafkaProducer

from MONO_SIGNAL.Data_Generation_MSES import DataGeneration_MSES


def json_serializer(data):
    return json.dumps(data).encode("utf-8")  # mess is serialized in json format


producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=json_serializer,
                         )

dg = DataGeneration_MSES()
while 1 == 1:
    ad = dg.get_event()
    print(ad)
    producer.send("MSES", ad)
    time.sleep(2)

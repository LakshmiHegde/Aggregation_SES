from faker import Faker
import json
import time, random
from datetime import datetime
import random


def randomInteger(start, end):
    y = random.randint(start, end)
    return y


class DataGeneration_BSES:

    def __init__(self):
        f = open('ADS.txt', "r")
        self.ads = []
        self.index= 0

        for ad in f:
            self.ads.append([ad.strip(), "start"])
            self.ads.append([ad.strip(), "end"])

        f.close()

    def assign_Time(self):
        t = datetime.now()
        hour = str(t.hour)
        minute = randomInteger(0, t.minute)
        sec = randomInteger(0, t.second)
        nano = randomInteger(0, t.microsecond)

        starttime = str(t.year) + "-" + str(t.month) + "-" + str(
            t.day) + " " + str(hour) + ":" + str(minute) + ":" + str(sec) + "." + str(nano)

        return starttime


    def get_event(self):
        ad_id = self.ads[randomInteger(0, len(self.ads)-1)]

        res = {
            "ad_id": ad_id[0],
            "tag": ad_id[1],
            "time": self.assign_Time()
        }
        #self.index = self.index+1
        #w = randomInteger(0 , 10)
        #print(w)
        #time.sleep(w)
        return res

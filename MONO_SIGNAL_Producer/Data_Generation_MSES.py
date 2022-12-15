
from datetime import datetime
import random


def randomInteger(start, end):
    y = random.randint(start, end)
    return y


class DataGeneration_MSES:
    def __init__(self):
        f = open('../ad_ids.txt', "r")
        self.ads = []
        for ad in f:
            self.ads.append(ad.strip())
        print(len(self.ads))
        f.close()

    def get_event(self):
        ad_id = self.ads[randomInteger(0, len(self.ads) - 1)]
        t = datetime.now()

        hour = str(t.hour)
        minute = randomInteger(0, t.minute)
        sec = randomInteger(0, t.second)
        nano = randomInteger(0, t.microsecond)

        ehour = randomInteger(hour, t.hour)
        emin = randomInteger(min, t.minute)
        esec = randomInteger(sec, t.second)
        enano = randomInteger(nano, t.microsecond)

        endtime = str(t.year) + "-" + str(t.month) + "-" + str(
            t.day) + " " + str(ehour) + ":" + str(emin) + ":" + str(esec) + "." + str(enano)

        starttime = str(t.year) + "-" + str(t.month) + "-" + str(
            t.day) + " " + str(hour) + ":" + str(minute) + ":" + str(sec) + "." + str(nano)

        return {
            "ad_id": ad_id,
            "start_time": starttime,
            "end_time": endtime
        }

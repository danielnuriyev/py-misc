#!/usr/bin/python
# -*- coding: utf-8 -*-

# https://pypi.org/project/twitter/#description
# https://github.com/dpkp/kafka-python

from twitter import Twitter, OAuth, TwitterHTTPError, TwitterStream
from kafka import KafkaProducer
import json

def set_oath():

    TAKEY = "247366579-RqlHio9rzQDpIp7FCz3HsOBZrgUHKVRgnss9gOt3"
    TASEC = "BYe0nvfuz6oqqJVflIRqF6mn9GLWhIlULkzqVROh6vsrx"
    CAKEY = "Sbi4n2nIUuF51452L7qKsLqul"
    CASEC = "6KVp5x6ksbXB5z5y3W0rTCzgBOS8cBEwo7mjjJUkdkKbwkZ74I"

    oauth = OAuth(TAKEY, TASEC, CAKEY, CASEC)

    return oauth


if __name__ == "__main__":

    oauth_t = set_oath()
    producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    while True:

        twitter_stream = TwitterStream(auth=oauth_t)
        records = twitter_stream.statuses.sample()

        for record in records:

            if 'hangup' in record or 'timeout' in record:
                break

            print(json.dumps(record))

            producer.send('test', json.dumps(record))

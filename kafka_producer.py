#!/usr/bin/env python3
# FraudDetection Kafka Producer and Consumer - part of Open Data Hub
# Copyright(C) 2019 Red Hat, Inc.
#
# This program is free software: you can redistribute it and / or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.


"""This is FraudDetection Kafka Producer - part of an Open Data Hub Tutorial."""


__version__ = "0.1.0"


import argparse
import json
import botocore
import boto3
import time
import random
import os
from kafka import KafkaProducer

DEFAULT_REGION = "us-east-1"


def fetchS3data(bucket, filename, accesskey, secretkey, s3endpoint):
    client = boto3.client(
        "s3",
        endpoint_url=s3endpoint,
        aws_access_key_id=accesskey,
        aws_secret_access_key=secretkey,
        region_name=DEFAULT_REGION,
        verify=False,
    )
    csv_obj = client.get_object(Bucket=bucket, Key=filename)
    class_one = []
    class_zero = []
    body = csv_obj["Body"]
    csv_string = body.read().decode("utf-8").splitlines()
    for each in csv_string[1:]:
        if each[-1] == "0":
            class_zero.append(each)
        else:
            class_one.append(each)

    return [class_zero, class_one]


def sendMessage(payload, topic, producer):
    producer.send(topic, payload)
    producer.flush()


def main():

    s3bucket = os.environ["s3bucket"]
    accesskey = os.environ["ACCESS_KEY_ID"]
    secretkey = os.environ["SECRET_ACCESS_KEY"]
    s3endpoint = os.environ["s3endpoint"]
    filename = os.environ["filename"]
    topic = os.environ["topic"]
    bootstrap = os.environ["bootstrap"]

    print(s3bucket, filename, s3endpoint)
    print("AK:", accesskey)
    print("SK:", secretkey)
    messages = fetchS3data(s3bucket, filename, accesskey, secretkey, s3endpoint)

    class_zero = messages[0]
    class_one = messages[1]

    producer = KafkaProducer(bootstrap_servers=bootstrap)
    one_pointer = 0
    zero_pointer = 0
    print(len(class_zero))
    print(len(class_one))

    while True:
        prob = random.randint(1, 6)
        if prob == 5:
            if one_pointer < len(class_one):
                sendMessage(json.dumps(class_one[one_pointer]).encode("utf-8"), topic, producer)
                one_pointer = one_pointer + 1
            else:
                one_pointer = 0
                sendMessage(json.dumps(class_one[one_pointer]).encode("utf-8"), topic, producer)
        else:
            if zero_pointer < len(class_zero):
                sendMessage(json.dumps(class_zero[zero_pointer]).encode("utf-8"), topic, producer)
                zero_pointer = zero_pointer + 1
            else:
                one_pointer = 0
                sendMessage(json.dumps(class_zero[zero_pointer]).encode("utf-8"), topic, producer)

        time.sleep(random.randint(2, 6))


# For more documentation on Producer: https://pypi.org/project/kafka-python/

if __name__ == "__main__":
    main()

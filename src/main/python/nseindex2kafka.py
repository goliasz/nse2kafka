#!/usr/bin/env python

# Copyright KOLIBERO under one or more contributor license agreements.  
# KOLIBERO licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import uuid
import argparse
import requests
import time
from datetime import datetime
from kafka import KafkaProducer
from nsetools import Nse

def send_msg(name, desc):
  q = nse.get_index_quote(name)
  if q:
    pChange = q.get("pChange")
    if not pChange:
      pChange = 0.0
    change = q.get("change")
    if not change:
      change = 0.0
    #
    msgj = {"pChange":pChange,
            "lastPrice":q.get("lastPrice"),
            "change":change,
            "type":"index",
            "name":name,
            "desc":desc,
            "timestamp":int(time.time() * 1000)}
    #
    producer.send(args.kafka_target_topic,msgj)
    print msgj

if __name__ == '__main__':
  parser = argparse.ArgumentParser(description="NSE index 2 kafka")
  parser.add_argument('--kafka_bootstrap_srvs', default="ccb:9092")
  parser.add_argument('--kafka_group_id', default="nseidx2kafka")
  parser.add_argument('--kafka_target_topic', default="nse")

  args = parser.parse_args()
  print "Kafka boostrap servers",args.kafka_bootstrap_srvs
  print "Kafka group id",args.kafka_group_id
  print "Kafka target topic",args.kafka_target_topic

  producer = KafkaProducer(bootstrap_servers=args.kafka_bootstrap_srvs,value_serializer=lambda v: json.dumps(v).encode('utf-8'))

  nse = Nse()
  index_codes = nse.get_index_list()
  for i in index_codes:
    print i
    send_msg(i, None)

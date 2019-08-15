#!/usr/bin/env python3

import http.server
import json
import kafka
import requests
import socketserver
import time

consul_url = 'http://consul:8500/v1/kv'


def wait_for_consul():
    while True:
        try:
            requests.head(consul_url)
            break
        except requests.exceptions.ConnectionError:
            print('Consul is not available ... will retry in 1s')
            time.sleep(1)
            pass


def consul_put_kv(key, value):
    r = requests.put(
        '{}/{}'.format(consul_url, key), data=json.dumps(value, indent=True))
    r.raise_for_status()


def write_config():
    """Write all needed configuration in Consul"""
    wait_for_consul()

    consul_put_kv('viyadb/clusters/cluster001/config', {
        'replication_factor': 2,
        'tables': ['events'],
        'indexers': ['main']
    })

    consul_put_kv(
        'viyadb/tables/events/config', {
            'name':
            'events',
            'dimensions': [
                {'name': 'app_id'}, \
                {'name': 'user_id', 'type': 'uint'}, \
                {'name': 'event_time', 'type': 'time', 'format': 'millis', 'granularity': 'day'}, \
                {'name': 'country'}, \
                {'name': 'city'}, \
                {'name': 'device_type'}, \
                {'name': 'device_vendor'}, \
                {'name': 'ad_network'}, \
                {'name': 'campaign'}, \
                {'name': 'site_id'}, \
                {'name': 'event_type'}, \
                {'name': 'event_name'}, \
                {'name': 'organic', 'cardinality': 2}, \
                {'name': 'days_from_install', 'type': 'ushort'} \
            ],
            'metrics': [
                {'name': 'revenue', 'type': 'double_sum'}, \
                {'name': 'users', 'type': 'bitset', 'field': 'user_id', 'max': 4294967295}, \
                {'name': 'count', 'type': 'count'} \
            ]
        })

    consul_put_kv(
        'viyadb/indexers/main/config', {
            'tables': ['events'],
            'deepStorePath': '/tmp/viyadb/deepstore',
            'realTime': {
                'windowDuration': 'PT20S',
                'kafkaSource': {
                    'topics': ['events'],
                    'brokers': ['kafka:9092']
                },
                'parseSpec': {
                    'format': 'json',
                    'timeColumn': {
                        'name': 'event_time'
                    }
                },
                'notifier': {
                    'type': 'kafka',
                    'channel': 'kafka:9092',
                    'queue': 'rt-notifications'
                }
            },
            'batch': {
                'batchDuration': 'PT1M',
                'partitioning': {
                    'columns': ['app_id'],
                    'partitions': 2
                },
                'notifier': {
                    'type': 'kafka',
                    'channel': 'kafka:9092',
                    'queue': 'batch-notifications'
                }
            }
        })


def wait_for_kafka():
    while True:
        try:
            return kafka.KafkaProducer(
                bootstrap_servers='kafka:9092',
                value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        except kafka.errors.NoBrokersAvailable:
            print('Kafka is not available ... will retry in 3s')
            time.sleep(3)


def init_kafka_notifications():
    """Setup Kafka notification topics initial contents"""
    producer = wait_for_kafka()

    columns = [
        'app_id', 'user_id', 'event_time', 'country', 'city', 'device_type',
        'device_vendor', 'ad_network', 'campaign', 'site_id', 'event_type',
        'event_name', 'organic', 'days_from_install', 'revenue', 'users',
        'count'
    ]

    rt_notifications = [
        {'id':1565439340000,'tables':{'events':{'paths':['/tmp/viyadb/deepstore/realtime/events/dt=1565439300000/mb=1565439340000'],'columns':columns,'recordCount':127}},'offsets':[{'topic':'events','partition':0,'offset':139}]}, \
        {'id':1565439360000,'tables':{'events':{'paths':['/tmp/viyadb/deepstore/realtime/events/dt=1565439360000/mb=1565439360000'],'columns':columns,'recordCount':181}},'offsets':[{'topic':'events','partition':0,'offset':349}]}, \
        {'id':1565439380000,'tables':{'events':{'paths':['/tmp/viyadb/deepstore/realtime/events/dt=1565439360000/mb=1565439380000'],'columns':columns,'recordCount':166}},'offsets':[{'topic':'events','partition':0,'offset':537}]}, \
        {'id':1565439400000,'tables':{'events':{'paths':['/tmp/viyadb/deepstore/realtime/events/dt=1565439360000/mb=1565439400000'],'columns':columns,'recordCount':178}},'offsets':[{'topic':'events','partition':0,'offset':743}]}, \
        {'id':1565439420000,'tables':{'events':{'paths':['/tmp/viyadb/deepstore/realtime/events/dt=1565439420000/mb=1565439420000'],'columns':columns,'recordCount':178}},'offsets':[{'topic':'events','partition':0,'offset':938}]}, \
        {'id':1565439440000,'tables':{'events':{'paths':['/tmp/viyadb/deepstore/realtime/events/dt=1565439420000/mb=1565439440000'],'columns':columns,'recordCount':177}},'offsets':[{'topic':'events','partition':0,'offset':1141}]}, \
        {'id':1565439460000,'tables':{'events':{'paths':['/tmp/viyadb/deepstore/realtime/events/dt=1565439420000/mb=1565439460000'],'columns':columns,'recordCount':179}},'offsets':[{'topic':'events','partition':0,'offset':1346}]}, \
    ]

    batch_notifications = [
        {'id':1565439300000,'tables':{'events':{'paths':['/tmp/viyadb/deepstore/batch/events/dt=1565439300000'],'columns':columns,'partitioning':[0,1],'partitionConf':{'columns':['app_id'],'partitions':2},'recordCount':127}},'microBatches':[1565439340000]}, \
        {'id':1565439360000,'tables':{'events':{'paths':['/tmp/viyadb/deepstore/batch/events/dt=1565439360000'],'columns':columns,'partitioning':[0,1],'partitionConf':{'columns':['app_id'],'partitions':2},'recordCount':612}},'microBatches':[1565439360000,1565439380000,1565439400000]}, \
        {'id':1565439420000,'tables':{'events':{'paths':['/tmp/viyadb/deepstore/batch/events/dt=1565439420000'],'columns':columns,'partitioning':[0,1],'partitionConf':{'columns':['app_id'],'partitions':2},'recordCount':1004}},'microBatches':[1565439420000,1565439440000,1565439460000]}, \
    ]

    for e in rt_notifications:
        producer.send('rt-notifications', e).get()

    for e in batch_notifications:
        producer.send('batch-notifications', e).get()


def start_webserver():
    """This webserver notifies other test components that everything is configured"""
    with socketserver.TCPServer(('0.0.0.0', 8080),
                                http.server.SimpleHTTPRequestHandler) as httpd:
        httpd.serve_forever()


if __name__ == '__main__':
    write_config()
    init_kafka_notifications()
    start_webserver()

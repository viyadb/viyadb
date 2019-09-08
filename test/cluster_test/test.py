#!/usr/bin/env python3

import csv
import json
import kafka
import requests
import time

nodes = ['viyadb_1', 'viyadb_2']

columns = [
    'app_id', 'user_id', 'event_time', 'country', 'city', 'device_type',
    'device_vendor', 'ad_network', 'campaign', 'site_id', 'event_type',
    'event_name', 'organic', 'days_from_install', 'revenue', 'users', 'count'
]


def read_cluster_config(host):
    """Return configuration written by ViyaDB node controller"""
    try:
        r = requests.get('http://{}:5555/config'.format(host))
        if r.status_code == 200:
            return r.json()
    except requests.exceptions.ConnectionError:
        pass
    return None


def wait_for_viyadb_cluster():
    """Check whether all the nodes are started by reading their controller config"""
    while True:
        time.sleep(3)
        for node in nodes:
            if not read_cluster_config(node):
                print(
                    'Not all ViyaDB nodes have started yet ... will retry in 3s'
                )
                break
        else:
            break
    print('ViyaDB cluster is ready!')


def wait_for_kafka():
    while True:
        try:
            return kafka.KafkaProducer(
                bootstrap_servers='kafka:9092',
                value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        except kafka.errors.NoBrokersAvailable:
            print('Kafka is not available ... will retry in 3s')
            time.sleep(3)


def send_new_notifications():
    """Send new micro-batches info to Kafka for triggering real-time ingestion"""
    producer = wait_for_kafka()

    new_notifications = [
        {'id':1565439480000,'tables':{'events':{'paths':['/tmp/viyadb/deepstore/realtime/events/dt=1565439480000/mb=1565439480000'],'columns':columns,'recordCount':156}},'offsets':[{'topic':'events','partition':0,'offset':1536}]}, \
        {'id':1565439500000,'tables':{'events':{'paths':['/tmp/viyadb/deepstore/realtime/events/dt=1565439480000/mb=1565439500000'],'columns':columns,'recordCount':173}},'offsets':[{'topic':'events','partition':0,'offset':1739}]}, \
        {'id':1565439520000,'tables':{'events':{'paths':['/tmp/viyadb/deepstore/realtime/events/dt=1565439480000/mb=1565439520000'],'columns':columns,'recordCount':174}},'offsets':[{'topic':'events','partition':0,'offset':1931}]}, \
        {'id':1565439540000,'tables':{'events':{'paths':['/tmp/viyadb/deepstore/realtime/events/dt=1565439540000/mb=1565439540000'],'columns':columns,'recordCount':161}},'offsets':[{'topic':'events','partition':0,'offset':2133}]}, \
        {'id':1565439560000,'tables':{'events':{'paths':['/tmp/viyadb/deepstore/realtime/events/dt=1565439540000/mb=1565439560000'],'columns':columns,'recordCount':170}},'offsets':[{'topic':'events','partition':0,'offset':2338}]}, \
        {'id':1565439580000,'tables':{'events':{'paths':['/tmp/viyadb/deepstore/realtime/events/dt=1565439540000/mb=1565439580000'],'columns':columns,'recordCount':173}},'offsets':[{'topic':'events','partition':0,'offset':2530}]}, \
        {'id':1565439600000,'tables':{'events':{'paths':['/tmp/viyadb/deepstore/realtime/events/dt=1565439600000/mb=1565439600000'],'columns':columns,'recordCount':174}},'offsets':[{'topic':'events','partition':0,'offset':2734}]}, \
        {'id':1565439620000,'tables':{'events':{'paths':['/tmp/viyadb/deepstore/realtime/events/dt=1565439600000/mb=1565439620000'],'columns':columns,'recordCount':170}},'offsets':[{'topic':'events','partition':0,'offset':2928}]}, \
    ]

    for e in new_notifications:
        producer.send('rt-notifications', e).get()


def send_sql_query(query, host, port, wait_for_batch=None):
    while True:
        try:
            r = requests.post(
                'http://{}:{}/sql'.format(host, port), data=query)
        except requests.exceptions.ConnectionError:
            print('Host {}:{} is not available ... will retry in 3s'.format(
                host, port))
            time.sleep(3)
            continue
        r.raise_for_status()
        if wait_for_batch:
            last_batch = r.headers.get("X-Last-Batch-ID")
            if last_batch and int(last_batch) < wait_for_batch:
                time.sleep(3)
                continue
        break
    return csv.reader(
        r.text.splitlines(), delimiter='\t', quoting=csv.QUOTE_NONE)


def compare_results(expected, actual):
    if expected != actual:
        raise Exception('Expected: {}\nActual: {}'.format(expected, actual))


def check_node_bootstrapped(host):
    compare_results({
        'com.skype.raider': '44'
    }, dict(send_sql_query(query, host, 5000, 1565439460000)))
    compare_results({
        'com.dropbox.android': '68'
    }, dict(send_sql_query(query, host, 5001, 1565439460000)))
    compare_results({
        'com.dropbox.android': '68',
        'com.skype.raider': '44'
    }, dict(send_sql_query(query, host, 5555)))


def check_node_uptodate(host):
    compare_results({
        'com.skype.raider': '76'
    }, dict(send_sql_query(query, host, 5000, 1565439620000)))
    compare_results({
        'com.dropbox.android': '164'
    }, dict(send_sql_query(query, host, 5001, 1565439620000)))
    compare_results({
        'com.dropbox.android': '164',
        'com.skype.raider': '76'
    }, dict(send_sql_query(query, host, 5555)))


def send_control_cmd(host, cmd):
    r = requests.get('http://{}:8080/{}'.format(host, cmd))
    r.raise_for_status()


if __name__ == '__main__':
    wait_for_viyadb_cluster()

    query = 'SELECT app_id,count FROM events WHERE app_id IN (\'com.dropbox.android\', \'com.skype.raider\')'
    for host in nodes:
        check_node_bootstrapped(host)

    send_new_notifications()

    time.sleep(3)
    for host in nodes:
        check_node_uptodate(host)

    send_control_cmd(nodes[0], 'kill_worker')
    check_node_uptodate(nodes[0])

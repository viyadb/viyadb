#!/usr/bin/env python2.7

from SimpleHTTPServer import SimpleHTTPRequestHandler
from BaseHTTPServer import HTTPServer
from subprocess import Popen, check_output

import threading
import socket
import urllib2
import time
import sys
import json
import os
import signal


class StatsDServer(threading.Thread):
    def __init__(self):
        self.received = []
        self.running = True
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.settimeout(0.2)
        self.sock.bind(('127.0.0.1', 8125))
        threading.Thread.__init__(self)

    def run(self):
        while self.running:
            try:
                data, _ = self.sock.recvfrom(8192)
                for metric in data.rstrip('\n').split('\n'):
                    self.received.append(metric)
            except socket.timeout:
                pass

    def stop(self):
        self.running = False
        self.join()


statsd = StatsDServer()


def run_viyadb(rack_id):
    while True:
        try:
            response = urllib2.urlopen('http://setup:8080/')
            break
        except urllib2.URLError:
            print('Configuration is not set ... will retry in 1s')
            time.sleep(1)

    config = {
        "supervise": True,
        "workers": 2,
        "cluster_id": "cluster001",
        "consul_url": "http://consul:8500",
        "rack_id": rack_id,
        "state_dir": "/tmp/viyadb",
        "http_port": 5000,
        "statsd": {
            "host": "127.0.0.1"
        }
    }
    config_file = '/tmp/store.json'

    with open(config_file, 'w') as f:
        json.dump(config, f)

    print('Starting ViyaDB node')
    Popen(['./src/server/viyad', config_file])


def kill_worker():
    pid = int(
        check_output(
            'ps -o pid= -C viyad  | sort -n | tail -n+2 | head -1',
            shell=True))
    print('Killing a ViyaDB worker (PID: {})'.format(pid))
    os.kill(pid, signal.SIGTERM)
    return {}


def statsd_metrics():
    global statsd
    return statsd.received


def start_server():
    class HttpHandler(SimpleHTTPRequestHandler):
        def do_GET(self):
            func = self.path[1:]
            res = {}
            if len(func) > 0:
                res = globals()[func]()
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(res).encode(encoding='utf_8'))

    server = HTTPServer(('0.0.0.0', 8080), HttpHandler)
    server.serve_forever()


if __name__ == '__main__':
    statsd.start()
    pid = run_viyadb(sys.argv[1])
    start_server()
    statsd.stop()

#!/usr/bin/env python2.7

from SimpleHTTPServer import SimpleHTTPRequestHandler
from BaseHTTPServer import HTTPServer
from subprocess import Popen, check_output

import urllib2
import time
import sys
import json
import os
import signal


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
        "http_port": 5000
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


def start_server():
    class HttpHandler(SimpleHTTPRequestHandler):
        def do_GET(self):
            func = self.path[1:]
            if len(func) > 0:
                globals()[func]()

            self.send_response(200)
            self.end_headers()

    server = HTTPServer(('0.0.0.0', 8080), HttpHandler)
    server.serve_forever()


if __name__ == '__main__':
    pid = run_viyadb(sys.argv[1])
    start_server()

__author__ = 'skoocda'
#status monitor client

import argparse
from ws4py.client.threadedclient import WebSocketClient
import time
import threading
import sys
import urllib
import Queue
import json
import time
import os


class StatusClient(WebSocketClient):

    def __init__(self, url, protocols=None, extensions=None, heartbeat_freq=None):
        super(StatusClient, self).__init__(url, protocols, extensions, heartbeat_freq)

    def send_data(self, data):
        self.send(data, binary=True)

    def opened(self):
        print("Opened status connection")

    def received_message(self, m):
        print("=> %d %s" % (len(m), str(m)))
        status = json.loads(str(m))
        workers = status.num_workers_available
        requests = status.num_requests_processed

    def closed(self, code, reason=None):
        print(("Closed down", code, reason))

def main():

if __name__ == "__main__":
    main()

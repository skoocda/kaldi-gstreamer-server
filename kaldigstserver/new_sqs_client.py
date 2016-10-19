__author__ = 'skoocda'
#deprecated

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

class SQSClient(WebSocketClient):

    def __init__(self, filename, url, protocols=None, extensions=None, heartbeat_freq=None):
        super(SQSClient, self).__init__(url, protocols, extensions, heartbeat_freq)
        self.final_hyps = []
        self.fn = filename
        self.byterate = byterate
        self.final_hyp_queue = Queue.Queue()

    def send_data(self, data):
        self.send(data, binary=True)

    def opened(self):
        print "[CLIENT] Socket opened!"
        def send_data_to_ws():
            f = open(self.fn, "rb")
            for block in iter(lambda: f.read(self.byterate/4), ""):
                self.send_data(block)
            self.send("EOS")

        t = threading.Thread(target=send_data_to_ws)
        t.start()

    def received_message(self, m):
        response = json.loads(str(m))
        if response['status'] == 0:
            if 'result' in response:
                trans = response['result']['hypotheses'][0]['transcript']
                if response['result']['final']:
                    #print >> sys.stderr, trans,
                    self.final_hyps.append(str(m)+' ')
                    #print >> sys.stderr, '\r%s' % trans.replace("\n", "\\n")
        else:
            print >> sys.stderr, "Received error from server (status %d)" % response['status']
            if 'message' in response:
                print >> sys.stderr, "Error message:",  response['message']

    def get_full_hyp(self, timeout=60):
        return self.final_hyp_queue.get(timeout)

    def closed(self, code, reason=None):
        #print "Websocket closed() called"
        #print >> sys.stderr
        self.final_hyp_queue.put(" ".join(self.final_hyps))


def main():

if __name__ == "__main__":
    main()

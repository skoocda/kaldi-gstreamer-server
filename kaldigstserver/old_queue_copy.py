__author__ = 'skoocda'

import os
import sys
import time
import thread
import logging
import json
import boto3
import botocore
import pymongo
from sys import stdout
from pymongo import MongoClient
from sqs_client import SQSClient
from ws4py.client.threadedclient import WebSocketClient
from ws4py.manager import WebSocketManager

manager = WebSocketManager()
timeout = 60
logging.basicConfig(filename='queue.log', filemode='w', level=logging.INFO)

############################### Get the service resources
sqs = boto3.resource('sqs')
s3 = boto3.resource('s3')

bucketName = 'spreza-audio'
queueIn = 'transcribe'
queueOut = 'complete'
#URI = '54.205.82.96'
URI = '54.221.51.233'
ASR_URI = 'ws://' + URI + '/client/ws/speech'
STATUS_URI = 'ws://' + URI + '/client/ws/status'
baseS3 = 'https://' + bucketName + '.s3.amazonaws.com/'

#english_test
test1 = 'mogdwzcmijaxapi1471346675086.wav'
#bill_gates-TED
test2 = 'btvetyxznumvnsq1473104501017.mp3'

#client = MongoClient('mongodb://spreza:spreza@ds049106-a0.mlab.com:49106,ds049106-a1.mlab.com:49106/spreza?replicaSet=rs-ds049106')
#db = client.spreza

client = MongoClient('mongodb://spreza:spreza@ds027215.mlab.com:27215/audio')
db = client.audio
transcripts = db.transcripts
users = db.users
statistics = db.statistics

########## global vars for tracking state
inProgress = {}
WORKER_COUNT = 0
REQUEST_COUNT = 0

class StatusClient(WebSocketClient):
    def __init__(self, url, protocols=None, extensions=None, heartbeat_freq=None,
                 ssl_options=None, headers=None):
        super(StatusClient, self).__init__(url, protocols, extensions, heartbeat_freq, ssl_options, headers)
        self.workers = 0
        self.requests = 0

    def handshake_ok(self):
        logging.info("[STATUS] Added status monitor to manager")
        manager.add(self)

    def opened(self):
        logging.info("[STATUS] Opened status monitor connection")

    def received_message(self, m):
        logging.info("[STATUS] %d => %s" % (len(m), str(m)))
        status = json.loads(str(m))
        self.workers = status['num_workers_available']
        self.requests = status['num_requests_processed']
        logging.info('[STATUS] Workers available: {}'.format(self.workers))
        #self.close()

    def closed(self, code, reason=None):
        logging.info(("[STATUS] Closed down", code, reason))

    def get_workers(self):
        return self.workers

    def get_requests(self):
        return self.requests

class ASRClient(SQSClient):
    def handshake_ok(self):
        manager.add(self)
        #add the websocketclient to the manager after handshake

    def received_message(self, m):
        response = json.loads(str(m))
        #print >> sys.stderr, "RESPONSE:", response
        #print >> sys.stderr, "JSON was:", m
        logging.info(m)
        if response['status'] == 0:
            if 'result' in response:
                trans = response['result']['hypotheses'][0]['transcript']
                if response['result']['final']:
                    #print >> sys.stderr, trans,
                    self.final_hyps.append(str(m)+' ')
                    #print >> sys.stderr, '\r%s' % trans.replace("\n", "\\n")
                else:
                    print_trans = trans.replace("\n", "\\n")
                    if len(print_trans) > 80:
                        print_trans = "... %s" % print_trans[-76:]
                    #print >> sys.stderr, '\r%s' % print_trans,
            if 'adaptation_state' in response:
                if self.save_adaptation_state_filename:
                    #print >> sys.stderr, "Saving adaptation state to %s" % self.save_adaptation_state_filename
                    with open(self.save_adaptation_state_filename, "w") as f:
                        f.write(json.dumps(response['adaptation_state']))
        else:
            logging.warning( "Received error from server (status {})".format(response['status']))
            if 'message' in response:
                logging.info( "Error message: {}".format(response['message']))

    def closed(self, code, reason=None):
        logging.info ("[UPDATE] Websocket closed() called")
        #print >> sys.stderr
        self.final_hyp_queue.put(" ".join(self.final_hyps))
        transcript = self.get_full_hyp()
        if transcript:
            logging.info ('[UPDATE] Received final transcript:')
            logging.info (transcript.encode('utf-8'))
            logging.info('[INFO] Result is in format: ')
            logging.info(type(transcript))
            key = self.fn[6:]
            if (update_db(transcript, key)):
                logging.info('[UPDATE] Successfully updated DB  ')
                inProgress[key] = 'DB'
                os.remove(self.fn)
                logging.info('[UPDATE] Sending response')
                queue_response(key)
            else:
                logging.warning('[ERROR] Could not find DB entry')
        #else:
        #    print('[ERROR] Could not get final hypothesis')
        return

def connect_queue(queueName):
    # Print out each queue name, which is part of its ARN
    #for queue in sqs.queues.all():
    #    print(queue.url)

    # Get the queue. This returns an SQS.Queue instance
    queue = sqs.get_queue_by_name(QueueName=queueName)
    if queue:
        logging.info('[UPDATE] Locked queue: {0}'.format(queueName))
    return queue

def connect_bucket():
    #for bucket in s3.buckets.all():
    #    print(bucket)

    bucket = s3.Bucket(bucketName)
    logging.info('[UPDATE] Got bucket {0}'.format(bucket.name))
    exists = True
    try:
        s3.meta.client.head_bucket(Bucket=bucketName)
    except botocore.exceptions.ClientError as e:
        # If a client error is thrown, then check that it was a 404 error.
        # If it was a 404 error, then the bucket does not exist.
        error_code = int(e.response['Error']['Code'])
        if error_code == 404:
            exists = False
            print('Does not exist')

    return bucket

def queue_response(msgAttribute):
    queue = connect_queue(queueOut)
    response = queue.send_message(
        MessageBody='complete',
        MessageAttributes={
            'file': {
                'StringValue': msgAttribute,
                'DataType': 'String'
            }
        }
    )
    logging.info('[UPDATE] Added response!')
    inProgress.pop(msgAttribute, None)
    return

def file_test(queue, msgAttribute):
    response = queue.send_message(
        MessageBody='transcribe',
        MessageAttributes={
            'file': {
                'StringValue': msgAttribute,
                'DataType': 'String'
            }
        }
    )
    logging.info('[UPDATE] Added test message!')
    logging.info(response)

def update_db(transcript, filename):
    #This is to treat multiple JSON objects as a single response
    transcriptRevised = transcript.replace('}  {', '},  {')
    dbEntry = '{"response": [ ' + transcriptRevised + '] }'
    logging.info('[INFO] Searching for file and updating DB:')
    logging.info(dbEntry)

    result = db.transcripts.update_one(
        {"audio.url": baseS3 + filename},
        {
            "$set": {
                "content.full": json.loads(dbEntry)
            }
        }
    )
    if (result.matched_count == 1):
        return True
    else:
        return False
        logging.warning('[ERROR] No db match found')

def mark_error(filename):
    result = db.transcripts.update_one(
        {"audio.url": baseS3 + filename},
        {
            "$set": {
                "status": "Error"
            }
        }
    )
    if (result.matched_count == 1):
        return True
    else:
        return False
        logging.warning('[ERROR] No db match found')

def get_job(queue, bucket):
    logging.info('[UPDATE] Getting new jobs...')
    for message in queue.receive_messages(MessageAttributeNames=['file']):
        logging.info(message)
        logging.info(message.body)
        logging.info(message.message_attributes)
        if message.message_attributes is not None:
            filepath = message.message_attributes.get('file').get('StringValue')
            if filepath:
                logging.info('[UPDATE] Found job: '+ message.body)
                filename = filepath[38:]
                try:
                    logging.info('[UPDATE] Downloading to ./tmp/{0}'.format(filename))
                    downloadpath = './tmp/'+filename
                    bucket.download_file(filename, downloadpath)
                    count = 0
                    while not os.path.exists(downloadpath) and count < 99:
                        time.sleep(1)
                        count += 1
                    if count == 99:
                        logging.error('[ERROR] Timeout during download')
                    if os.path.isfile(downloadpath):
                        inProgress[filename] = 'start'
                        # Let the queue know that the message is processed
                        message.delete()
                        logging.info('[UPDATE] Deleting message: {0}'.format(filename))
                        # All systems are go. Initiate ASR decoding.
                        run_ASR(filename)
                    else:
                        raise ValueError("%s isn't a file!" % downloadpath)

                except botocore.exceptions.ClientError as e:
                    logging.error('[ERROR] Botocore exception -- File is not there')
                    mark_error(filename)
                    message.delete()
            else:
                logging.warning('[ERROR] No file found')

def run_ASR(filename):
    logging.info('[UPDATE] Starting ASR on file {0}'.format(filename))
    filepath = './tmp/'+filename
    ws = ASRClient(filepath, ASR_URI)
    logging.info('[UPDATE] Initiated ASR connection')
    ws.connect()
    logging.info('[UPDATE] Connected to ASR server')
    inProgress[filename] = 'transcribe'


def loop(transcribe, bucket, sm):
    logging.info('[UPDATE] Set up.')
    worker_count = 0
    while True:
        worker_count = sm.get_workers()
        stdout.write('-POLLING- {} workers available. \r'.format(worker_count))
        stdout.flush()
        if worker_count > 0:
            worker_count -= 1
            get_job(transcribe, bucket)
        #sm.close()
            #for x in inProgress:
            #    stdout.write('{} : {} \n'.format(x, inProgress[x]))

def main():
    manager.start()
    logging.info('[UPDATE] Started manager')
    transcribe = connect_queue(queueIn)
    complete = connect_queue(queueOut)
    bucket = connect_bucket()
    sm = StatusClient(STATUS_URI)
    sm.connect()
    #time.sleep(10)
    #file_test(transcribe, test1)
    try:
        loop(transcribe, bucket, sm)
    except KeyboardInterrupt:
        # We're done. Bail out without dumping a traceback.
        with open('progress.log', 'w') as f:
            f.write(json.dumps(inProgress))
        manager.close_all()
        manager.stop()
        manager.join()
        stdout.write("\n")
        sys.exit(0)

if __name__ == "__main__":
    main()

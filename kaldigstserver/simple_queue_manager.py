__author__ = 'skoocda'

import os
import sys
import time
import thread
import json
import boto3
import botocore
import pymongo
from sys import stdout
from pymongo import MongoClient
from sqs_client import SQSClient
from ws4py.manager import WebSocketManager

manager = WebSocketManager()
timeout = 60

# Get the service resources
sqs = boto3.resource('sqs')
s3 = boto3.resource('s3')

bucketName = 'spreza-audio'
queueIn = 'transcribe'
queueOut = 'complete'
ASR_URI = 'ws://54.221.51.233/client/ws/speech'
baseS3 = 'https://spreza-audio.s3.amazonaws.com/'

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

class ASRClient(SQSClient):
    def handshake_ok(self):
        manager.add(self)

    def closed(self, code, reason=None):
        self.final_hyp_queue.put(" ".join(self.final_hyps))
        transcript = self.get_full_hyp()
        if transcript:
            key = self.fn[6:]
            if (update_db(transcript, key)):
                os.remove(self.fn)
                queue_response(key)
        return

def connect_queue(queueName):
    # Get the queue. This returns an SQS.Queue instance
    queue = sqs.get_queue_by_name(QueueName=queueName)
    if queue:
        print('Got queue {0}'.format(queue.url))

    return queue

def connect_bucket():
    bucket = s3.Bucket(bucketName)
    print('Got bucket {0}'.format(bucket.name))
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
    print('[UPDATE] Added test message!')
    #print(response)

def update_db(transcript, filename):
    transcriptRevised = transcript.replace('}  {', '},  {')
    dbEntry = '{"response": [ ' + transcriptRevised + '] }'

    result = db.transcripts.update_one(
        {"audio.url": baseS3 + filename},
        {"$set": {"content.full": json.loads(dbEntry)}})
    if (result.matched_count == 1):
        return True
    else:
        return False
        print('[ERROR] No db match found')

def get_job(queue, bucket):
    for message in queue.receive_messages(MessageAttributeNames=['file']):
        if message.message_attributes is not None:
            filepath = message.message_attributes.get('file').get('StringValue')
            if filepath:
                filename = filepath[38:]
                try:
                    bucket.download_file(filename, './tmp/'+filename)
                    if os.path.isfile('./tmp/'+filename):
                        # Let the queue know that the message is processed
                        message.delete()
                        # All systems are go. Initiate ASR decoding.
                        run_ASR(filename)

                except botocore.exceptions.ClientError as e:
                    print('[ERROR] Botocore exception -- File is not there')
            else:
                print('[ERROR] No file found')

def run_ASR(filename):
    filepath = './tmp/'+filename
    ws = ASRClient(filepath, ASR_URI)
    ws.connect()
    return


def loop(transcribe, bucket):
    print ('[UPDATE] Initialized. Polling for jobs...')
    while True:
        get_job(transcribe, bucket)

def main():
    transcribe = connect_queue(queueIn)
    complete = connect_queue(queueOut)
    bucket = connect_bucket()
    manager.start()
    try:
        loop(transcribe, bucket)
    except KeyboardInterrupt:
        # We're done. Bail out without dumping a traceback.
        manager.close_all()
        manager.stop()
        manager.join()
        stdout.write("\n")
        sys.exit(0)

if __name__ == "__main__":
    main()

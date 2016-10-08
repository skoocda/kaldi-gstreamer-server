__author__ = 'skoocda'

import os
import sys
import thread
import json
import boto3
import botocore
import pymongo
from pymongo import MongoClient
from sqs_client import SQSClient
from ws4py.manager import WebSocketManager

m = WebSocketManager()

sqs = boto3.resource('sqs')
s3 = boto3.resource('s3')

bucketName = 'spreza-audio'
queueIn = 'transcribe'
queueOut = 'complete'
ASR_URI = 'ws://54.221.51.233/client/ws/speech'
baseS3 = 'https://spreza-audio.s3.amazonaws.com/'

client = MongoClient('mongodb://spreza:spreza@ds049106-a0.mlab.com:49106,ds049106-a1.mlab.com:49106/spreza?replicaSet=rs-ds049106')

class ASRClient(SQSClient):

    def handshake_ok(self):
        m.add(self)

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
    queue = sqs.get_queue_by_name(QueueName=queueName)
    return queue

def connect_bucket():
    bucket = s3.Bucket(bucketName)
    exists = True
    try:
        s3.meta.client.head_bucket(Bucket=bucketName)
    except botocore.exceptions.ClientError as e:
        error_code = int(e.response['Error']['Code'])
        if error_code == 404:
            exists = False
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

def update_db(transcript, filename):
    transcriptRevised = transcript.replace('}  {', '},  {')
    dbEntry = '{"response": [ ' + transcriptRevised + '] }'
    result = client.spreza.transcripts.update_one(
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

def get_job(queue, bucket):
    for message in queue.receive_messages(MessageAttributeNames=['file']):
        if message.message_attributes is not None:
            filename = message.message_attributes.get('file').get('StringValue')
            if filename:
                bucket.download_file(filename, './tmp/'+filename)
                message.delete()
                return filename

def run_ASR(filename):
    filepath = './tmp/'+filename
    ws = ASRClient(filepath, ASR_URI)
    ws.connect()

def loop(transcribe, bucket):
    while True:
        filename = get_job(transcribe, bucket)
        if filename:
            run_ASR(filename)

def main():
    transcribe = connect_queue(queueIn)
    bucket = connect_bucket()
    m.start()
    try:
        loop(transcribe, bucket)
    except KeyboardInterrupt:
        m.close_all()
        m.stop()
        m.join()
        sys.exit(0)

if __name__ == "__main__":
    main()

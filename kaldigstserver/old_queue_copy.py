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

#list of files being processed through this manager
fileState = {}


class ASRClient(SQSClient):

    def handshake_ok(self):
        manager.add(self)
        #add the websocketclient to the manager after handshake

    def received_message(self, m):
        response = json.loads(str(m))
        #print >> sys.stderr, "RESPONSE:", response
        #print >> sys.stderr, "JSON was:", m
        #print >> sys.stderr, m
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
            print >> sys.stderr, "Received error from server (status %d)" % response['status']
            if 'message' in response:
                print >> sys.stderr, "Error message:",  response['message']

    def closed(self, code, reason=None):
        print "[UPDATE] Websocket closed() called"
        #print >> sys.stderr
        self.final_hyp_queue.put(" ".join(self.final_hyps))
        transcript = self.get_full_hyp()
        if transcript:
            print ('[UPDATE] Received final transcript:')
            #print transcript.encode('utf-8')
            #print('[INFO] Result is in format: ')
            #print(type(transcript))
            key = self.fn[6:]
            if (update_db(transcript, key)):
                print('[UPDATE] Successfully updated DB  ')
                fileState[key] = 1
                os.remove(self.fn)
                print('[UPDATE] Sending response')
                queue_response(key)
            else:
                print('[ERROR] Could not find DB entry')
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
        print('[UPDATE] Locked queue: {0}'.format(queueName))

    # You can now access identifiers and attributes
    #print(queue.url)
    #print(queue.attributes.get('DelaySeconds'))

    return queue

def connect_bucket():
    #for bucket in s3.buckets.all():
    #    print(bucket)

    bucket = s3.Bucket(bucketName)
    print('[UPDATE] Got bucket {0}'.format(bucket.name))
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
    print('[UPDATE] Added response!')
    #print(response)
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

    #f = open('test.txt', 'r+')
    #f.write(transcript)
    #print(transcript)

    #This is to treat multiple JSON objects as a single response
    transcriptRevised = transcript.replace('}  {', '},  {')
    #objs = json.loads("[%s]"%(transcriptRevised))
    dbEntry = '{"response": [ ' + transcriptRevised + '] }'
    #dbEntry = transcriptRevised
    #print('[INFO] Searching for: {0}').format(baseS3 + filename)
    #print('Updating with: {0}').format(dbEntry)

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
        print('[ERROR] No db match found')

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
        print('[ERROR] No db match found')

def get_job(queue, bucket):
    for message in queue.receive_messages(MessageAttributeNames=['file']):
        #print(message)
        #print(message.body)
        #print(message.message_attributes)
        if message.message_attributes is not None:
            filepath = message.message_attributes.get('file').get('StringValue')
            if filepath:
                print('[UPDATE] Found job: '+ message.body)
                filename = filepath[38:]
                try:
                    print('[UPDATE] Downloading to ./tmp/{0}'.format(filename))
                    downloadpath = './tmp/'+filename
                    bucket.download_file(filename, downloadpath)
                    count = 0
                    while not os.path.exists(downloadpath) and count < 99:
                        time.sleep(1)
                        count += 1
                    if count == 99:
                        print('[ERROR] Timeout during download')
                    if os.path.isfile(downloadpath):
                        fileState[filename] = 0
                        # Let the queue know that the message is processed
                        message.delete()
                        print('[UPDATE] Deleting message: {0}'.format(filename))
                        # All systems are go. Initiate ASR decoding.
                        run_ASR(filename)
                    else:
                        raise ValueError("%s isn't a file!" % downloadpath)

                except botocore.exceptions.ClientError as e:
                    print('[ERROR] Botocore exception -- File is not there')
                    mark_error(filename)
                    message.delete()
            else:
                print('[ERROR] No file found')

def run_ASR(filename):
    print ('[UPDATE] Starting ASR on file {0}'.format(filename))
    filepath = './tmp/'+filename
    ws = ASRClient(filepath, ASR_URI)
    print ('[UPDATE] Initiated ASR connection')
    ws.connect()
    print ('[UPDATE] Connected to ASR server')

    #ws.on_close
    return

def loop(transcribe, bucket):
    print ('[UPDATE] Set up. Polling for jobs:')
    while True:
        get_job(transcribe, bucket)
        #stdout.write('\r')
        #for x in fileState:
        #    stdout.write('{} : {} \n'.format(x, fileState[x]))
        #stdout.flush()

def main():
    transcribe = connect_queue(queueIn)
    complete = connect_queue(queueOut)
    bucket = connect_bucket()
    manager.start()
    #file_test(transcribe, test1)
    try:
        loop(transcribe, bucket)
    except KeyboardInterrupt:
        # We're done. Bail out without dumping a traceback.
        with open('jobs.json', 'w') as f:
            f.write(json.dumps(fileState))
        manager.close_all()
        manager.stop()
        manager.join()
        stdout.write("\n")
        sys.exit(0)

if __name__ == "__main__":
    main()

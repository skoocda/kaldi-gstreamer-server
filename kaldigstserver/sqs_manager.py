__author__ = 'skoocda'


import json
import boto3
import botocore
from sqs_client import SQSClient
wsClient = SQSClient()
sqs = boto3.resource('sqs')
s3 = boto3.resource('s3')
audioBucket = s3.Bucket('spreza-audio')
exists = True
try:
    s3.meta.client.head_bucket(Bucket='spreza-audio')
except botocore.exceptions.ClientError as e:
    # If a client error is thrown, then check that it was a 404 error.
    # If it was a 404 error, then the bucket does not exist.
    error_code = int(e.response['Error']['Code'])
    if error_code == 404:
        exists = False

#import pymongo
#from pymongo import MongoClient
#client = MongoClient('mongodb://spreza:spreza@@ds049106-a0.mlab.com:49106,ds049106-a1.mlab.com:49106/spreza?replicaSet=rs-ds049106')
#should create a user just for this python client
#Create symbolic variables for each database segment
#db = client.spreza
#transcripts = db.transcripts
#users = db.users
#statistics = db.statistics


#t = sqs.get_queue_by_name(QueueName= "TODO")
#r = sqs.get_queue_by_name(QueueName= "RESPONSE")

"python kaldigstserver/client.py -r 8192 test/data/bill_gates-TED.mp3"
"python kaldigstserver/client.py -r 32000 --save-adaptation-state adaptation-state.json test/data/english_test.wav"
"python kaldigstserver/client.py -r 32000 --send-adaptation-state adaptation-state.json test/data/english_test.wav"

#response = r.send_message(MessageBody='boto3', MessageAttributes={
#    ''
#})

"need to set server URI in client"
##########################################################################################

class QueueManager(object)
    """
    This is the main queue manager class, which exposes a ``run`` method
    to put everything into motion. It watches for new messages, downloads
    them from S3, converts them using Spreza Transcriber and saves
    the output to mongo.
    """
    # The following policies are for the IAM role.
    queue_policy_statement = {
        "Sid": "transcribe",
        "Effect": "Allow",
        "Principal": {
            "AWS": "*"
        },
        "Action": "SQS:SendMessage",
        "Resource": "<SQS QUEUE ARN>",
        "Condition": {
            "StringLike": {
                "aws:SourceArn": "<SNS TOPIC ARN>"
            }
        }
    }
    def __init__(self,
                 active_directory,
                 transcriber
                 bucket_name='spreza-audio',
                 role_name='transcribe-user',
                 topic_name='transcribe-complete',
                 queue_name='transcribe',
                 poll_interval=10,
                 region_name='us-east-1'):
        super(QueueManager, self).__init__()

        # Local related.
        self.active_directory = active_directory

        # Transcription related
        self.transcriber =

        # AWS related.
        self.bucket_name = bucket_name
        self.region_name = region_name
        self.role_name = role_name
        self.topic_name = topic_name
        self.queue_name = queue_name
        self.role_arn = None
        self.topic_arn = None
        self.queue_arn = None

        self.bucket = None
        self.role = None
        self.queue = None

        # How often should we look at the local FS for updates?
        self.poll_interval = int(poll_interval)

        self.s3 = boto3.resource('s3')
        self.sns = boto3.resource('sns', self.region_name)
        self.sqs = boto3.resource('sqs', self.region_name)

    def ensure_local_setup(self):
        """
        Ensures that the local directory setup is sane by making sure
        that the directories exist
        """

        if not os.path.exists(self.active_directory):
            os.makedirs(self.active_directory)
        else:
            # If it's there, it may already have files in it, which may have
            # already been processed. Keep these filenames & only process
            # new ones.
            self.existing_files = set(self.collect_files())

    def ensure_aws_setup(self):
        """
        Ensures that the AWS services, resources, and policies are set
        up so that they can all talk to one another and so that we
        can transcribe audio files.
        """
        if self.bucket_exists(self.bucket_name):
            self.bucket = self.s3.Bucket(self.bucket_name)
        else:
            self.bucket = self.s3.create_bucket(
                Bucket=self.bucket_name)

        self.topic_arn = self.get_sns_topic()
        self.queue = self.get_sqs_queue()

    def start_transcribing(self, files_found):
        """
        Download and transcribe each file. Uploads are processed in series
        while transcribing happens in parallel.
        """
        for filepath in files_found:
            filename = self.download_from_s3(filepath)
            self.start_transcribe(filename)
            self.existing_files.add(filepath)

    def process_completed(self):
        """
        Check the queue and download any completed files from S3 to your
        hard drive.
        """
        to_fetch = self.check_queue()

        for s3_file in to_fetch:
            self.download_from_s3(s3_file)

    def collect_files(self):
        """
        Get a list of all relevant files (based on the file extension)
        in the local unconverted media file directory.
        """
        path = os.path.join(self.unconverted_directory, self.file_pattern)
        return glob.glob(path)

    def bucket_exists(self, bucket_name):
        """
        Returns ``True`` if a bucket exists and you have access to
        call ``HeadBucket`` on it, otherwise ``False``.
        """
        try:
            self.s3.meta.client.head_bucket(Bucket=bucket_name)
            return True
        except ClientError:
            return False

    def get_sns_topic(self):
        """
        Get or create the SNS topic.
        """
        # Creating a topic is idempotent, so if it already exists
        # then we will just get the topic returned.
        return self.sns.create_topic(Name=self.topic_name).arn

    def get_sqs_queue(self):
        """
        Get or create the SQS queue. If it is created, then it is
        also subscribed to the SNS topic, and a policy is set to allow
        the SNS topic to send messages to the queue.
        """
        # Creating a queue is idempotent, so if it already exists
        # then we will just get the queue returned.
        queue = self.sqs.create_queue(QueueName=self.queue_name)
        self.queue_arn = queue.attributes['QueueArn']

        # Ensure that we are subscribed to the SNS topic
        subscribed = False
        topic = self.sns.Topic(self.topic_arn)
        for subscription in topic.subscriptions.all():
            if subscription.attributes['Endpoint'] == self.queue_arn:
                subscribed = True
                break

        if not subscribed:
            topic.subscribe(Protocol='sqs', Endpoint=self.queue_arn)

        # Set up a policy to allow SNS access to the queue
        if 'Policy' in queue.attributes:
            policy = json.loads(queue.attributes['Policy'])
        else:
            policy = {'Version': '2008-10-17'}

        if 'Statement' not in policy:
            statement = self.queue_policy_statement
            statement['Resource'] = self.queue_arn
            statement['Condition']['StringLike']['aws:SourceArn'] = \
                self.topic_arn
            policy['Statement'] = [statement]

            queue.set_attributes(Attributes={
                'Policy': json.dumps(policy)
            })

        return queue

    def start_transcribe(self, filename):
        """
        Submit a job to transcode a file by its filename. The
        built-in web system preset is used for the single output.
        """
        self.transcoder.create_job(
            PipelineId=self.pipeline_id,
            Input={
                'Key': filename,
                'FrameRate': 'auto',
                'Resolution': 'auto',
                'AspectRatio': 'auto',
                'Interlaced': 'auto',
                'Container': 'auto'
            },
            Outputs=[{
                'Key': '.'.join(filename.split('.')[:-1]) + '.mp4',
                'PresetId': '1351620000001-100070'
            }]
        )
        print("Started transcoding {0}".format(filename))


        ws = self.SQSClient(
            audiofile,
            uri + '?%s' % (urllib.urlencode([("content-type", content_type)])),
            byterate=64000)
        ws.connect()
        output = ws.get_full_hyp()
        #print result.encode('utf-8')

    def check_queue(self):
        """
        Check the queue for new files and set them to be
        downloaded.
        """
        queue = self.queue
        to_fetch = []

        for msg in queue.receive_messages(WaitTimeSeconds=self.poll_interval):
            body = json.loads(msg.body)
            message = body.get('Message', '{}')
            files = json.loads(message).get('files', [])

            if not len(files):
                print("Saw no files in {0}".format(body))
                continue

            key = files[0].get('key')

            if not key:
                print("Saw no key in files in {0}".format(body))
                continue

            if msg

            to_fetch.append(key)
            print("Completed {0}".format(key))
            #msg.delete()

        return to_fetch

    def download_from_s3(self, s3_file):
        """
        Download a file from the S3 output bucket to your hard drive.
        """
        destination_path = os.path.join(
            self.transcribe_directory,
            os.path.basename(s3_file)
        )
        body = self.bucket.Object(s3_file).get()['Body']
        with open(destination_path, 'wb') as dest:
            # Here we write the file in chunks to prevent
            # loading everything into memory at once.
            for chunk in iter(lambda: body.read(4096), b''):
                dest.write(chunk)

        print("Downloaded {0}".format(destination_path))

    def send_message(self, transcriptId):
        """
        Send a response message into the queue
        """
        queue = self.queue
        to_send = []
        self.queue.send_message(
            MessageBody='complete',
            MessageAttributes= {
                'TranscriptId': transcriptId
        })
        print("Started transcribing {0}".format(transcriptId))

    def run(self):
        """
        Start the main loop. This repeatedly checks for new files,
        uploads them and starts jobs if needed, and checks for and
        downloads completed files. It sleeps for ``poll_interval``
        seconds between checks.
        """
        # Make sure everything we need is setup, both locally & on AWS.
        self.ensure_local_setup()
        self.ensure_aws_setup()

        # Run forever, or until the user says stop.
        while True:
            print("Checking for new messages.")
            files_found = self.check_queue()

            if files_found:
                print("Found {0} new file(s).".format(len(files_found)))
                self.start_transcribing(files_found[0])

            # Here we check the queue, which will long-poll
            # for up to ``self.poll_interval`` seconds.
            self.process_completed()

def main():
    import sys
    try:
        QueueManager.run()
    except KeyboardInterrupt:
        # We're done. Bail out without dumping a traceback.
        sys.exit(0)


if __name__ == "__main__":
    main()

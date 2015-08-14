__author__ = 'nkdhny'

import luigi
import os.path
import tempfile
import json
import boto3
import boto3.session
import runtag.preprocessor as pre
import runtag.helpers as hlp
import cv2
import collections
import uuid
import time
import logging
from sqs import CreateResponseQueue


class CallDetection(luigi.Task):
    # task to build up detection list
    source = luigi.Parameter()

    id = luigi.Parameter()
    detection_type = luigi.Parameter()

    sqs_name = luigi.Parameter()
    sqs_callback_name = luigi.Parameter()

    aws_session = luigi.Parameter()
    aws_region = luigi.Parameter()

    max_allowed_shapes_per_message = luigi.IntParameter(default=100)

    def requires(self):
        return {
            'source': self.source,
            'response_queue': CreateResponseQueue(
                sqs_callback_name=self.sqs_callback_name,
                aws_session=self.aws_session
            )
        }

    def output(self):
        path = os.path.join(tempfile.gettempdir(), 'runtag-detection-call.{}.json'.format(self.id))
        return luigi.LocalTarget(path=path)

    def _get_queue(self):
        sqs = self.aws_session.resource('sqs')

        return sqs.get_queue_by_name(QueueName=self.sqs_name)

    def _send_part(self, queue, part, message_id):
        queue.send_message(MessageBody=json.dumps(map(hlp.marker_to_hash, part)), MessageAttributes={
            'id': {
                'StringValue': message_id,
                'DataType': 'String'
            },
            'type': {
                'StringValue': self.detection_type,
                'DataType': 'String'
            },
            'callback': {
                'StringValue': 'sqs://{}/{}'.format(self.aws_region, self.sqs_callback_name),
                'DataType': 'String'
            },
            'kind': {
                'StringValue': 'preprocessed',
                'DataType': 'String'
            }
        })

    def _gen_message_id(self):
        return str(uuid.uuid4())

    def run(self):
        detection_list = json.load(self.input()['source'].open())
        messages_published = 0

        q = self._get_queue()
        messages = {}

        for detection_item in detection_list:
            image = cv2.imread(detection_item, 0)
            candidates = pre.candidates(image)

            logging.debug("For image {} got {} candidates".format(detection_item, len(candidates)))

            if not candidates:
                continue

            head = candidates[:self.max_allowed_shapes_per_message]
            tail = candidates[self.max_allowed_shapes_per_message:]

            while head:
                message_id = self._gen_message_id()

                self._send_part(q, head, message_id)
                logging.debug("Successfully sent page of {} candidates".format(len(head)))
                messages[message_id] = detection_item

                head = tail[:self.max_allowed_shapes_per_message]
                tail = tail[self.max_allowed_shapes_per_message:]

        with self.output().open('w') as o:
            json.dump(messages, o)

        logging.debug("Stored detection calls in {}".format(self.output().path))

class WaitForDetection(luigi.Task):
    wait_timeout = luigi.IntParameter(default=60)  # in seconds
    # CallDetection Parameters
    source = luigi.Parameter()

    id = luigi.Parameter()
    detection_type = luigi.Parameter()

    sqs_name = luigi.Parameter()
    sqs_callback_name = luigi.Parameter()

    aws_session = luigi.Parameter()
    aws_region = luigi.Parameter()

    def requires(self):
        return CallDetection(source=self.source, id=self.id, detection_type=self.detection_type, sqs_name=self.sqs_name,
                             sqs_callback_name=self.sqs_callback_name, aws_session=self.aws_session,
                             aws_region=self.aws_region)

    def output(self):
        path = os.path.join(tempfile.gettempdir(), 'runtag-detection-result.{}.json'.format(self.id))
        return luigi.LocalTarget(path=path)

    @staticmethod
    def _tick(queue, detection_messages, detection_results):
        messages = queue.receive_messages(MaxNumberOfMessages=10, MessageAttributeNames=['All'], WaitTimeSeconds=10)

        for message in messages:
            message_id = message.message_attributes['id']['StringValue']
            if message_id in detection_messages:
                logging.debug("Processing response for message {}".format(message_id))
                markers_serial = message.body

                markers = json.loads(markers_serial)

                detection_item = detection_messages[message_id]
                detection_results[detection_item] += markers

                logging.debug("Got {} markers on detection item {}".format(len(markers), detection_item))

                del detection_messages[message_id]
                message.delete()

                logging.debug("Removed message {}".format(message_id))

    def _get_queue(self):
        sqs = self.aws_session.resource('sqs')

        return sqs.get_queue_by_name(QueueName=self.sqs_callback_name)

    def run(self):
        detection_result = collections.defaultdict(lambda: [])

        queue = self._get_queue()

        detection_messages = json.load(self.input().open())

        start_time = time.time()

        while start_time + self.wait_timeout > time.time() and detection_messages:
            logging.debug("Reading messages, up to 10 at time")
            WaitForDetection._tick(queue, detection_messages, detection_result)

        with self.output().open('w') as o:
            json.dump(detection_result, o)

        logging.debug("Stored detection result in {}".format(self.output().path))

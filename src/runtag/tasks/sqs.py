__author__ = 'nkdhny'

import luigi

class CreateResponseQueue(luigi.Task):
    sqs_callback_name = luigi.Parameter()
    aws_session = luigi.Parameter()

    def complete(self):

        sqs = self.aws_session.resource('sqs')

        try:
            return sqs.get_queue_by_name(QueueName=self.sqs_callback_name) is not None
        except:
            return False

    def run(self):
        sqs = self.aws_session.resource('sqs')

        sqs.create_queue(
            QueueName=self.sqs_callback_name,
            Attributes={
                'MessageRetentionPeriod': '600',
                 'ReceiveMessageWaitTimeSeconds': '20'
            }
        )

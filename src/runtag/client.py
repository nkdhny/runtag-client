import os
import sys
import uuid
import logging
from tasks.detect import WaitForDetection
from tasks.source import FilesInDirectory
import yaml
import luigi.interface as interface
import luigi.scheduler as scheduler
import luigi.worker as worker
import getopt
import boto3.session

__author__ = 'nkdhny'



def configure_logging(verbose=False):
    log_level = logging.INFO
    if verbose:
        log_level = logging.DEBUG

    logging.basicConfig(format='%(levelname)s:%(message)s', level=log_level)

if __name__ == '__main__':
    opts, args = getopt.getopt(sys.argv[1:], "p:d:c:s:t:v")
    opts_d = dict(opts)
    configure_logging('-v' in opts_d)
    conf = yaml.load(file(opts_d.get('-c', '/etc/runtag/client.yaml')), Loader=yaml.Loader)


    dir = opts_d.get('-d', os.path.curdir)
    pattern = opts_d.get('-p', '*.jpg')

    logging.info("Will process {} files in {}".format(pattern, dir))

    secret_key = opts_d.get('-s', None)

    if secret_key is None:
        print >> sys.stderr, "AWS secret key not specified, specify it with -s option"
        sys.exit(-1)

    detection_type = opts_d.get('-t', 'secded')
    logging.info('Will use {} detector'.format(detection_type))

    sqs_name = conf['aws']['detector']['queue']

    aws_region = conf['aws']['region']
    aws_key_id = conf['aws']['account']['key']

    aws_account_id = conf['aws']['account']['name']

    detection_id = uuid.uuid4()
    response_queue_name = aws_account_id
    logging.info("Detection id is {} response queue {}".format(detection_id, response_queue_name))

    source = FilesInDirectory(dir=dir, id=detection_id, file_pattern=pattern)
    detection = WaitForDetection(
        source=source,
        id=detection_id,
        detection_type=detection_type,
        sqs_name= sqs_name,
        sqs_callback_name=response_queue_name,
        aws_session=boto3.session.Session(aws_access_key_id=aws_key_id, aws_secret_access_key=secret_key,
                                            region_name=aws_region),
        aws_region=aws_region
    )

    interface.setup_interface_logging()
    sch = scheduler.CentralPlannerScheduler()
    w = worker.Worker(scheduler=sch)
    w.add(detection)
    w.run()

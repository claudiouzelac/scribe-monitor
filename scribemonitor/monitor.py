import argparse
import logging

from .statsd_helper import StatsD
from .monitors.file_store import FileStoreMonitor
from .monitors.status import StatusMonitor
from .monitors.hdfs_store import HdfsStoreMonitor


DEFAULT_STATSD_HOST = 'localhost'
DEFAULT_STATSD_PORT = 8125
DEFAULT_STATSD_PREFIX = 'scribe'

DEFAULT_SCRIBE_HOST = 'localhost'
DEFAULT_SCRIBE_PORT = 1463


logger = logging.getLogger()


def parse_options():
    parser = argparse.ArgumentParser(description='Monitor scribe server.')

    parser.add_argument('--file-store-path', help='Path to the file store location')

    parser.add_argument('--ctrl-host', default=DEFAULT_SCRIBE_HOST, help='Scribe thrift host')
    parser.add_argument('--ctrl-port', default=DEFAULT_SCRIBE_PORT, type=int, help='Scribe thrift port')

    parser.add_argument('--hdfs-path', help='Path to log files on hdfs')

    parser.add_argument('--statsd-host', default=DEFAULT_STATSD_HOST)
    parser.add_argument('--statsd-port', type=int, default=DEFAULT_STATSD_PORT)
    parser.add_argument('--statsd-prefix', default=DEFAULT_STATSD_PREFIX)

    parser.add_argument('--logger')

    return parser.parse_args()


def configure_logger(options):
    if options.logger == 'DEBUG':
        logger.setLevel(logging.DEBUG)
    elif options.logger == 'INFO':
        logger.setLevel(logging.INFO)
    elif options.logger == 'WARNING':
        logger.setLevel(logging.WARNING)
    else:
        logger.setLevel(logging.ERROR)

    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(name)s: %(message)s')
    handler.setFormatter(formatter)

    logger.addHandler(handler)


def run_monitors():
    options = parse_options()
    configure_logger(options)
    statsd = StatsD(options)

    fs_monitor = FileStoreMonitor(options, statsd)
    fs_monitor.start()

    status_monitor = StatusMonitor(options, statsd)
    status_monitor.start()

    hdfs_monitor = HdfsStoreMonitor(options, statsd)
    hdfs_monitor.start()

    fs_monitor.join()
    status_monitor.join()
    hdfs_monitor.join()

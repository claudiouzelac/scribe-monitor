import os
import datetime
import time
import threading
import socket
import argparse
import logging
import subprocess
import shlex
from collections import defaultdict

import statsd as statsd_module

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from fb303 import FacebookService
from fb303.ttypes import fb_status


DEFAULT_CHECK_WAIT = 1.0
HDFS_CHECK_WAIT = 30.0

DEFAULT_STATSD_HOST = 'localhost'
DEFAULT_STATSD_PORT = 8125
DEFAULT_STATSD_PREFIX = 'scribe'

DEFAULT_SCRIBE_HOST = 'localhost'
DEFAULT_SCRIBE_PORT = 1463

SCRIBE_STATUS_OK = 0
SCRIBE_STATUS_WARNING = 1
SCRIBE_STATUS_ERROR = 2


logger = logging.getLogger(__name__)


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

    logger.addHandler(logging.StreamHandler())


class StatsD(object):

    def __init__(self, options):
        self._statsc = statsd_module.StatsClient(
            options.statsd_host, options.statsd_port, prefix=options.statsd_prefix)

        self._source = socket.gethostname().replace('.', '')

    def timer(self, name, value):
        logger.info("%s=%s", name, value)
        self._statsc.timing('{0}.{1}'.format(self._source, name), value)

    def incr(self, name, value):
        logger.info("%s=%s", name, value)
        self._statsc.incr('{0}.{1}'.format(self._source, name), value)

    def gauge(self, name, value):
        logger.info("%s=%s", name, value)
        self._statsc.gauge('{0}.{1}'.format(self._source, name), value)


class FileStoreMonitor(threading.Thread):

    def __init__(self, options, statsd):
        self._statsd = statsd
        self._options = options

        super(FileStoreMonitor, self).__init__()
        self.daemon = True

    def check_store_size(self):
        size = 0

        for d, sd, files in os.walk(self._options.file_store_path):
            for f in files:
                filepath = os.path.join(d, f)
                try:
                    size += int(round(os.path.getsize(filepath) / 1E3))
                except OSError:
                    logger.warning('Invalid file %s', filepath)

        self._statsd.incr('file_store_size', size)

    def run(self):
        if not self._options.file_store_path:
            return

        while True:
            self.check_store_size()
            time.sleep(DEFAULT_CHECK_WAIT)


class StatusMonitor(threading.Thread):

    def __init__(self, options, statsd):
        self._statsd = statsd
        self._options = options

        self._counters = defaultdict(int)

        super(StatusMonitor, self).__init__()
        self.daemon = True

    def _update_counters(self, counters):
        for k, v in counters.iteritems():
            self._counters[k] = v

    def _clean_counter_keys(self, counters):
        new_counters = {}

        for k, v in counters.iteritems():
            nk = k.replace(':', '.').replace(' ', '_')
            new_counters[nk] = v

        return new_counters

    def _parse_counters(self, counters):
        counters = self._clean_counter_keys(counters)

        if not self._counters:
            self._update_counters(counters)
            return

        for k, v in counters.iteritems():
            dv = v - self._counters[k]
            self._statsd.incr(k, dv)

        self._update_counters(counters)

    def _parse_status(self, status):
        if status == fb_status.ALIVE:
            self._statsd.gauge('status', SCRIBE_STATUS_OK)
        elif status == fb_status.WARNING:
            self._statsd.gauge('status', SCRIBE_STATUS_WARNING)
        else:
            self._statsd.gauge('status', SCRIBE_STATUS_ERROR)

    def check_status(self):
        sock = TSocket.TSocket(self._options.ctrl_host, self._options.ctrl_port)

        trans = TTransport.TFramedTransport(sock)
        prot = TBinaryProtocol.TBinaryProtocol(trans)

        try:
            trans.open()
            fb303_client = FacebookService.Client(prot, prot)

            self._parse_status(fb303_client.getStatus())
            self._parse_counters(fb303_client.getCounters())
        except TSocket.TTransportException:
            logger.exception('Failed to open thrift socket')
            self._statsd.gauge('status', SCRIBE_STATUS_ERROR)
            return

        finally:
            trans.close()

    def run(self):
        while True:
            self.check_status()
            time.sleep(DEFAULT_CHECK_WAIT)


class HdfsMonitor(threading.Thread):

    def __init__(self, options, statsd):
        self._statsd = statsd
        self._options = options

        self._written_total = 0
        self._read_date = None

        super(HdfsMonitor, self).__init__()
        self.daemon = True

    def _get_search_path(self, date):
        return os.path.join(
            self._options.hdfs_path,
            '*',
            '*-{date}*'.format(date=date),
        )

    def check_store_size(self):
        size = 0
        size_today = 0
        today = datetime.date.today().isoformat()

        search_path = []
        search_path.append(self._get_search_path(today))
        if self._read_date and self._read_date != today:
            search_path.append(self._get_search_path(self._read_date))

        try:
            ls = subprocess.Popen(
                ['hadoop', 'fs', '-ls'] + search_path,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        except OSError:
            logger.exception('Hadoop command not found')
            self._statsd.incr('file_write', 0)
            return

        for line in ls.stdout:
            if line.find(self._options.hdfs_path) < 0:
                continue

            values = shlex.split(line)

            file_size = int(values[4])
            size += file_size

            if values[7].find(today) >= 0:
                size_today += file_size

        if self._read_date is None:
            self._read_date = today
            self._written_total = size_today
            return

        size_diff = size - self._written_total
        self._read_date = today
        self._written_total = size_today

        size_diff_kb = int(round(size_diff / 1E3))

        self._statsd.incr('hdfs_write', size_diff_kb)

    def run(self):
        if not self._options.hdfs_path:
            return

        while True:
            self.check_store_size()
            time.sleep(HDFS_CHECK_WAIT)


def run_monitor():
    options = parse_options()
    statsd = StatsD(options)
    configure_logger(options)

    fs_monitor = FileStoreMonitor(options, statsd)
    status_monitor = StatusMonitor(options, statsd)
    hdfs_monitor = HdfsMonitor(options, statsd)

    fs_monitor.start()
    status_monitor.start()
    hdfs_monitor.start()

    fs_monitor.join()
    status_monitor.join()
    hdfs_monitor.join()

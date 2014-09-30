import threading
import time
import logging
from collections import defaultdict

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from fb303 import FacebookService
from fb303.ttypes import fb_status


STATUS_INTERVAL = 1.0

SCRIBE_STATUS_OK = 0
SCRIBE_STATUS_WARNING = 1
SCRIBE_STATUS_ERROR = 2

logger = logging.getLogger(__name__)


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
            time.sleep(STATUS_INTERVAL)

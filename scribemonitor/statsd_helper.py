import socket
import logging

import statsd as statsd_module


logger = logging.getLogger(__name__)


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

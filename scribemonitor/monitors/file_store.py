import time
import threading
import os
import logging


FILE_STORE_INTERVAL = 1.0

logger = logging.getLogger(__name__)


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
            time.sleep(FILE_STORE_INTERVAL)

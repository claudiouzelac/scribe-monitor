import datetime
import os
import time
import shlex
import threading
import subprocess
import logging


HDFS_INTERVAL = 30.0


logger = logging.getLogger(__name__)


class HdfsStoreMonitor(threading.Thread):

    def __init__(self, options, statsd):
        self._statsd = statsd
        self._options = options

        self._written_total = 0
        self._read_date = None

        super(HdfsStoreMonitor, self).__init__()
        self.daemon = True

    def _get_search_path(self, date):
        return os.path.join(
            self._options.hdfs_path,
            '*',
            '*-{date}*'.format(date=date),
        )

    def _run_hadoop_ls(self, search_paths):
        try:
            return subprocess.Popen(
                ['hadoop', 'fs', '-ls'] + search_paths,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        except OSError:
            logger.exception('Hadoop command not found')
            return

    def _parse_ls_output(self, ls, today):
        size = size_today = 0

        for line in ls.stdout:
            if line.find(self._options.hdfs_path) < 0:
                continue

            values = shlex.split(line)

            file_size = int(values[4])
            size += file_size

            if values[7].find(today) >= 0:
                size_today += file_size

        return size, size_today

    def check_store_size(self):
        today = datetime.date.today().isoformat()

        search_paths = [self._get_search_path(today)]
        if self._read_date and self._read_date != today:
            search_paths.append(self._get_search_path(self._read_date))

        ls = self._run_hadoop_ls(search_paths)

        if ls is None:
            self._statsd.incr('hdfs_write', 0)
            return

        size, size_today = self._parse_ls_output(ls, today)

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
            time.sleep(HDFS_INTERVAL)

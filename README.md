scribe-monitor
==============

Monitor scribe server to statsd

- Scribe process status (logged to statsd under gauges)
- Value of scribe counters (packages received, sent, ...)
- Total file size of the file store (useful for monitoring secondary store)
- Data written to hdfs (checks all buckets on hdfs and reports size diff to statsd)

Example:
--------

    scribe_monitor --file-store-path=/home/scribe --statsd-host=scribe

**Primary store goes down**

![Primary store goes down](/docs/upstream-down.png?raw-true")

    scribe_monitor --hdfs-path hdfs://example/user/me/logs/ --file-store-path=/home/scribe --statsd-host=scribe

**Hdfs stops accepting writes**

![Hdfs stops accepting writes](/docs/hdfs-down.png?raw-true)

Install:
--------

    pip install scribe-monitor

Options:
--------

    --file-store-path FILE_STORE_PATH
        Path to the file store location
    --ctrl-host CTRL_HOST
        Scribe thrift host
    --ctrl-port CTRL_PORT
        Scribe thrift port
    --hdfs-path HDFS_PATH
        Path to log files on hdfs
    --statsd-host STATSD_HOST
    --statsd-port STATSD_PORT
    --statsd-prefix STATSD_PREFIX
    --logger LOGGER

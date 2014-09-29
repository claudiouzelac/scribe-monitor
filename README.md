scribe-monitor
==============

Monitor scribe server to statsd

- Scribe process status
- Value of scribe counters (packages received, sent, ...)
- Total file size of the file store (useful for monitoring secondary store)


Example:
--------

    scribe_monitor --secondary-store=/home/scribe --statsd-host=scribe


Options:
--------

    --file-store-path FILE_STORE_PATH
        Path to the file store location
    --log-file LOG_FILE
        Path to the log file
    --ctrl-host CTRL_HOST
        Scribe thrift host
    --ctrl-port CTRL_PORT
        Scribe thrift port
    --statsd-host STATSD_HOST
    --statsd-port STATSD_PORT
    --statsd-prefix STATSD_PREFIX
    --logger LOGGER

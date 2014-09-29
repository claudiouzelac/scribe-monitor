scribe-monitor
==============

Monitor scribe server to statsd

- Scribe process status (logged to statsd under gauges)
- Value of scribe counters (packages received, sent, ...)
- Total file size of the file store (useful for monitoring secondary store)

Example:
--------

    scribe_monitor --secondary-store=/home/scribe --statsd-host=scribe

**Primary store goes down**

![Primary store goes down](/docs/upstream-down.png?raw-true "promary store goes down")

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
    --statsd-host STATSD_HOST
    --statsd-port STATSD_PORT
    --statsd-prefix STATSD_PREFIX
    --logger LOGGER

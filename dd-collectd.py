import collectd
from datadog import statsd, initialize

DOGSTATSD_PLUGIN_INSTANCE = "dogstatsd_pluginInstance"
DOGSTATSD_TYPE_INSTANCE = "dogstatsd_typeInstance"

initialize(statsd_host="localhost", statsd_port=8125)


def write_callback(vl):

    metric = vl.plugin + "." + vl.type
    tags = []
    if vl.meta:
        if DOGSTATSD_PLUGIN_INSTANCE in vl.meta and vl.plugin_instance:
            tags.append("%s:%s" % (vl.meta.get(DOGSTATSD_PLUGIN_INSTANCE), vl.plugin_instance))
        if DOGSTATSD_TYPE_INSTANCE in vl.meta and vl.type_instance:
            tags.append("%s:%s" % (vl.meta.get(DOGSTATSD_TYPE_INSTANCE), vl.type_instance))

    value = vl.values[-1]

    metric_type = collectd.get_dataset(vl.type)

    if metric_type == collectd.DS_TYPE_COUNTER:
        statsd.increment(metric, value, tags)
    elif metric_type == collectd.DS_TYPE_GAUGE:
        statsd.gauge(metric, value, tags)
    elif metric_type == collectd.DS_TYPE_DERIVE:
        statsd.gauge(metric, value, tags)
    elif metric_type == collectd.DS_TYPE_ABSOLUTE:
        statsd.gauge(metric, value, tags)


# register callbacks
collectd.register_write(write_callback)
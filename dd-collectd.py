import collectd
import yaml
from datadog import statsd, initialize

DEBUG = False

initialize(statsd_host="localhost", statsd_port=8125)


class CollectdSubmitter(object):

    def __init__(self):
        self.is_initialized = False
        self.namespace = None
        self.plugins_data = None
        self.plugins = None
        self.aliases = None

    def init_config(self, config):
        config_file = None
        for node in config.children:
            log("Node is {}:{}".format(node, node.values[0]))
            key = node.key.lower()
            if key == 'config_file':
                config_file = node.values[0]

        if not config_file:
            raise Exception("Please set the config_file option.")

        with open(config_file, 'r') as f:
            config = yaml.load(f)
            self.namespace = config['namespace']
            self.plugins_data = config['plugins']
            self.plugins = [p['name'] for p in self.plugins_data]
            self.aliases = config.get('aliases', [])
            self.is_initialized = True
            log("Init done")

    def write_callback(self, vl):
        metric = "collectd." + vl.plugin + "." + vl.type
        plugin_data = self.plugins_data.get(vl.plugin)
        if not plugin_data:
            return

        if vl.plugin in self.plugins:
            log("Found metric {}.".format(metric))
        tags = []

        if "plugin_instance" in plugin_data and vl.plugin_instance:
            tags.append("{}:{}".format(plugin_data['plugin_instance'], vl.plugin_instance))
        if "type_instance" in plugin_data and vl.type_instance:
            tags.append("{}:{}".format(plugin_data['type_instance'], vl.plugin_instance))

        metric_type = collectd.get_dataset(vl.type)[1]
        log("Tags are {}".format(str(tags)))
        if 'values_to_suffix' in plugin_data:
            suffixes = plugin_data['values_to_suffix']
            for idx, value in enumerate(vl.values):
                if idx == len(suffixes):
                    break
                metric_name = metric + "." + suffixes[idx]
                self.submit_metric(metric_name, value, metric_type, tags)
        elif 'values_to_tags' in plugin_data:
            additional_tags = plugin_data['values_to_tags']
            for idx, value in enumerate(vl.values):
                if idx == len(additional_tags):
                    break
                self.submit_metric(metric, value, metric_type, tags + [additional_tags[idx]])

        elif vl.values:
            sum = 0
            for v in vl.values:
                sum += v
            self.submit_metric(metric, float(sum)/len(vl.values), metric_type, tags)

        metric_type = collectd.get_dataset(vl.type)[1]

    def submit_metric(self, metric_name, metric_value, metric_type, tags):
        metric_name = "collectd." + self.aliases(metric_name, metric_name)
        log("Submitting metric {}:{}:{}:{}".format(metric_name, metric_value, metric_type, tags))
        if metric_type == collectd.DS_TYPE_COUNTER:
            statsd.increment(metric_name, metric_value, tags)
        elif metric_type == collectd.DS_TYPE_GAUGE:
            statsd.gauge(metric_name, metric_value, tags)
        elif metric_type == collectd.DS_TYPE_DERIVE:
            statsd.gauge(metric_name, metric_value, tags)
        elif metric_type == collectd.DS_TYPE_ABSOLUTE:
            statsd.gauge(metric_name, metric_value, tags)
        else:
            collectd.error("Unknown metric type %s" % metric_type)


submitter = CollectdSubmitter()
# register callbacks
collectd.register_write(submitter.write_callback)
collectd.register_config(submitter.init_config)


def log(msg):
    collectd.error(msg)

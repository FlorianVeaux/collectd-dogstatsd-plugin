import collectd
import yaml
from datadog import statsd, initialize

VALUES_AS_TAG = 'values_names_as_tag'


class DogstatsdSubmitter(object):

    def __init__(self):
        self.is_initialized = False
        self.namespace = None
        self.plugins = None
        self.aliases = None
        self.is_verbose = False

    def init_config(self, config):
        config_file = None
        statsd_host = "localhost"
        statsd_port = 8125

        for node in config.children:
            key = node.key.lower()
            if key == 'config_file':
                config_file = node.values[0]
            elif key == 'dogstatsd_host':
                statsd_host = node.values[0]
            elif key == 'dogstatsd_port':
                statsd_port = node.values[0]
            elif key == 'verbose':
                self.is_verbose = node.values[0].lower() in ('true', 'yes')

        if not config_file:
            raise Exception("Please set the config_file option.")

        initialize(statsd_host=statsd_host, statsd_port=statsd_port)
        self.log_verbose("datadog library initialized to {}:{}".format(statsd_host, statsd_port))
        with open(config_file, 'r') as f:
            config = yaml.load(f)
            self.namespace = config['namespace']
            self.plugins = config['plugins']
            self.aliases = config.get('aliases', [])
            self.is_initialized = True
            collectd.info("write_dogstatsd plugin intialized!")

    @staticmethod
    def _get_tags(vl, plugin_cfg):
        tags = ["plugin:{}".format(vl.plugin), "type:{}".format(vl.type)]
        if vl.plugin_instance:
            tag_key = plugin_cfg.get('plugin_instance') or 'plugin_instance'
            tags.append("{}:{}".format(tag_key, vl.plugin_instance))
        if vl.type_instance:
            tag_key = plugin_cfg.get('type_instance') or 'type_instance'
            tags.append("{}:{}".format(tag_key, vl.type_instance))
        return tags

    @staticmethod
    def _read_dataset(vl):
        dataset = [d[0:2] for d in collectd.get_dataset(vl.type)]
        return [d[0] for d in dataset], [d[1] for d in dataset]

    def write_callback(self, vl):
        if vl.plugin not in self.plugins:
            return

        self.log_verbose("Trying to convert and submit the metric %s.%s", vl.plugin, vl.type)
        plugin_cfg = self.plugins[vl.plugin]
        # Get the metric name, either the alias if defined in the config or a mix of plugin + type
        metric = plugin_cfg.get("aliases", {}).get(vl.type) or "{}.{}".format(vl.plugin, vl.type)
        self.log_verbose("Metric %s.%s has been translated to %s", vl.plugin, vl.type, metric)

        values_names, types = DogstatsdSubmitter._read_dataset(vl)
        tags = self._get_tags(vl, plugin_cfg)
        nb_of_metrics = min(len(vl.values), len(types))

        if len(vl.values) == 1:
            self.submit_metric(metric, vl.values[0], types[0], tags)
        elif VALUES_AS_TAG in plugin_cfg:
            # Only keep as much as the number of available values.
            self.log_verbose("Submitting %d values for metric %s by adding tags.", nb_of_metrics, metric)
            tag_key = plugin_cfg.get(VALUES_AS_TAG)
            for i in range(nb_of_metrics):
                total_tags = tags + ["{}:{}".format(tag_key, values_names[i])]
                self.submit_metric(metric, vl.values[i], types[i], total_tags)
        else:
            # Only keep as much as the number of available values.
            self.log_verbose("Submitting %d values for metric %s by appendix suffix.", nb_of_metrics, metric)

            # Do not submit more metrics than there are suffix
            for i in range(nb_of_metrics):
                metric_name = metric + "." + values_names[i]
                self.submit_metric(metric_name, vl.values[i], types[i], tags)

    def submit_metric(self, metric_name, metric_value, metric_type, tags):
        metric_name = "{}.{}".format(self.namespace, metric_name)
        self.log_verbose("Submitting metric {}".format(metric_name))
        if metric_type in (collectd.DS_TYPE_COUNTER, collectd.DS_TYPE_DERIVE):
            statsd.increment(metric_name, metric_value, tags)
        elif metric_type in (collectd.DS_TYPE_GAUGE, collectd.DS_TYPE_ABSOLUTE):
            statsd.gauge(metric_name, metric_value, tags)
        else:
            collectd.error("Unknown metric type %s" % metric_type)

    def log_verbose(self, msg, *args):
        if self.is_verbose:
            collectd.info("[write_dogstatsd] {}".format(msg) % args)


submitter = DogstatsdSubmitter()
# register callbacks
collectd.register_write(submitter.write_callback)
collectd.register_config(submitter.init_config)


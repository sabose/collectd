import collectd
from gluster_utils import GlusterStats, get_gluster_cluster_topology
from gluster_plugins import all
import socket


list_plugins = []


def config_func(config):
    # collectd.info("gluster config received")
    GlusterStats.CONFIG = {c.key: c.values[0] for c in config.children}
    if 'peer_name' not in GlusterStats.CONFIG:
        hostname = socket.gethostname()
        ''' lets split hostname.domainname '''
        host_domain_fields = hostname.split('.')
        GlusterStats.CONFIG['peer_name'] = host_domain_fields[0]


def read_func():
    GlusterStats.CLUSTER_TOPOLOGY = get_gluster_cluster_topology()
    global threads
    global list_plugins
    # load plugins here
    for mod in all:
        full_name = "gluster_plugins.%s" % (mod)
        module_name = full_name.rpartition(".")[0]
        class_name = mod.split(".")[-1]
        module = __import__(module_name, globals(), locals(), [class_name])
        plugin = getattr(module, class_name)
        p = plugin()
        list_plugins.append(p)
    # run the plugin
    for plugin in list_plugins:
        plugin.run()

collectd.register_config(config_func)
collectd.register_read(read_func)

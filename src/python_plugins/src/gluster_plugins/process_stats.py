import collectd
from gluster_utils import GlusterStats, get_pids, get_cpu_memory_metrics, CollectdValue


class ProcessStats(GlusterStats):
    def run(self):
        for process in ["glusterd", "glusterfs"]:
            pids = get_pids(process)
            for pid in pids:
                cpu, memobj = get_cpu_memory_metrics(pid)
                # send cpu metrics
                plugin_instance = process
                cvalue = CollectdValue(self.plugin, plugin_instance, "cpu",
                                       [cpu], str(pid))
                cvalue.dispatch()

                # send rss metrics for memory
                cvalue = CollectdValue(self.plugin, plugin_instance, "ps_rss",
                                       [memobj.rss], str(pid))
                cvalue.dispatch()

                # send virtual memory stats
                cvalue = CollectdValue(self.plugin, plugin_instance, "ps_vm",
                                       [memobj.vms], str(pid))
                cvalue.dispatch()

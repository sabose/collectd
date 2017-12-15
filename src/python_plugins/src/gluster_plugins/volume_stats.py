import collectd
import time
import traceback
# import threading
from gluster_utils import GlusterStats, CollectdValue, exec_command


ret_val = {}


class VolumeStats(GlusterStats):
    def __init__(self):
        GlusterStats.__init__(self)

    def _parse_self_heal_stats(self, op):
        info = op.split('Crawl statistics for brick no ')
        bricks_heal_info = []
        for i in range(1, len(info)):
            brick_i_info = info[i].split('\n')
            brick_heal_info = {}
            for idx, line in enumerate(brick_i_info):
                line = line.strip()
                if idx == 0:
                    brick_heal_info['brick_index'] = int(line) + 1
                if 'No. of entries healed' in line:
                    brick_heal_info['healed_cnt'] = int(
                        line.replace('No. of entries healed: ', '')
                    )
                if 'No. of entries in split-brain' in line:
                    brick_heal_info['split_brain_cnt'] = int(
                        line.replace('No. of entries in split-brain: ', '')
                    )
                if 'No. of heal failed entries' in line:
                    brick_heal_info['heal_failed_cnt'] = int(
                        line.replace('No. of heal failed entries: ', '')
                    )
                if 'Hostname of brick ' in line:
                    brick_heal_info['host_name'] = line.replace(
                        'Hostname of brick ', ''
                    )
            bricks_heal_info.append(brick_heal_info)
        return bricks_heal_info

    def get_volume_heal_info(self, vol):
        ret_val = []
        for trial_cnt in xrange(0, 3):
            vol_heal_op, vol_heal_err = \
                exec_command("gluster volume heal %s statistics" % vol['name'])
            if vol_heal_err:
                time.sleep(5)
                if trial_cnt == 2:
                    collectd.error('Failed to fetch volume heal statistics.'
                                   'The error is: %s' % (
                                       vol_heal_err)
                                   )
                    return ret_val
                continue
            else:
                break
        try:
            vol_heal_info = self._parse_self_heal_stats(vol_heal_op)
            for idx, brick_heal_info in enumerate(vol_heal_info):
                for sub_vol_id, sub_vol in vol['bricks'].iteritems():
                    for brick_idx, sub_vol_brick in enumerate(sub_vol):
                        if (
                            sub_vol_brick['brick_index'] ==
                            brick_heal_info['brick_index']
                        ):
                            vol_heal_info[idx]['brick_path'] = \
                                sub_vol_brick['path']
                            ret_val.append(vol_heal_info[idx])
            return ret_val
        except (AttributeError, KeyError, ValueError):
            collectd.error('Failed to collect volume heal statistics.\
                           Error %s' % (
                traceback.format_exc()
                           )
                           )
            return ret_val

    def get_heal_info(self, volume):
        vol_heal_info = self.get_volume_heal_info(volume)
        list_values = []
        for brick_heal_info in vol_heal_info:
            brick_path = brick_heal_info['brick_path']
            hostname = brick_heal_info['host_name']

            t_name = "brick_split_brain_cnt"
            split_brain_cnt = brick_heal_info['split_brain_cnt']
            cvalue = CollectdValue(self.plugin, brick_path, t_name,
                                   [split_brain_cnt], None)
            cvalue.hostname = hostname
            list_values.append(cvalue)

            t_name = "brick_healed_cnt"
            healed_cnt = brick_heal_info['healed_cnt']
            cvalue = CollectdValue(self.plugin, brick_path, t_name,
                                   [healed_cnt], None)
            cvalue.hostname = hostname
            list_values.append(cvalue)

            t_name = "brick_heal_failed_cnt"
            heal_failed_cnt = brick_heal_info['heal_failed_cnt']
            cvalue = CollectdValue(self.plugin, brick_path, t_name,
                                   [heal_failed_cnt], None)
            cvalue.hostname = hostname
            list_values.append(cvalue)
        return list_values

    def run(self):
        # dont collect stats if volume_stats is turned off
        if 'volume_stats' not in self.CONFIG or \
                self.CONFIG['volume_stats'] == "false":
            return
        for volume in self.CLUSTER_TOPOLOGY.get('volumes', []):
            if 'Replicate' in volume.get('type', ''):
                list_values = self.get_heal_info(volume)
                for collectd_value in list_values:
                    collectd_value.dispatch()

                # thread = threading.Thread(
                # target=get_heal_info,
                # args=(volume, CONFIG['integration_id'],)
                # )
                # thread.start()
                # threads.append(
                # thread
                # )
                # for thread in threads:
                #    thread.join(1)
                # for thread in threads:
                #    del thread

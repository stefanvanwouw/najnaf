import subprocess
import multiprocessing
import re
import os
import time
import conf

class ResourceMonitor(object):
    def __init__(self):
        self._disk = 0
        self._mem = 0
        self._cpu = 0
        self._disk_last_time = 0
        self._runtime_last_time = 0

    def _get_disk(self):
        if time.time() - self._disk_last_time > conf.PERSISTENT_REFRESH:
            # refresh
            self._refresh_persistent_resources()

        return self._disk

    def _get_cpu(self):
        if time.time() - self._disk_last_time > conf.RUNTIME_REFRESH:
            # refresh
            self._refresh_runtime_resources()

        return self._cpu

    def _get_mem(self):
        if time.time() - self._disk_last_time > conf.RUNTIME_REFRESH:
            # refresh
            self._refresh_runtime_resources()

        return self._mem

    disk = property(_get_disk)
    cpu = property(_get_cpu)
    mem = property(_get_mem)


    def _refresh_runtime_resources(self):
        p = subprocess.Popen(["top", "-b", "-n1"], stdout=subprocess.PIPE)
        stdout,stderr = p.communicate()
        lines = stdout.split('\n',4)
        del lines[4]

        # cpu load.
        load_str = lines[0].split(":")[-1].split(",")
        load1,load5,load15 = [float(s) for s in load_str]

        # Memory.
        mem_str = lines[3].split(",",2)
        del mem_str[2]
        total_mem,used_mem = [float(re.sub(r'\D','', s)) for s in mem_str]
        self._cpu = load1/multiprocessing.cpu_count()
        self._mem = used_mem/total_mem

    def _refresh_persistent_resources(self):
        st = os.statvfs("/") # Assuming disk is attached to /.
        total = float(st.f_blocks * st.f_frsize)
        used = float((st.f_blocks - st.f_bfree) * st.f_frsize)
        self._disk = used/total

    def refresh(self):
        self._refresh_runtime_resources()
        self._refresh_persistent_resources()

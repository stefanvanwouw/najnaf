import conf, time, logging, socket, pickle, math, sys
from monitor import VMMonitor
from shared import AppState, AppCommand
from shared.packets import ProducerType
from vm_wrapper import OneVMWrapper, VMCommand, VMState, VMType

# TODO: The class currently assumes the VMs are not suddenly created or deleted
# by another program.
class VMScheduler(object):
    

    def __init__(self, monitor):
        self._monitor = monitor
        self._hb_buffer = {}
        self._cmds = {}
        self._wrapper = OneVMWrapper()
        self._vms = self._wrapper.list_vms()
        self._log = logging.getLogger("VMScheduler")
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._wait_for_master = True
        self._num_workers = 0
        self._start_times = {}
        self._time_started = int(time.time())

        master_exists = False
        worker_exists = False
        for id in self._vms[0]:
            type = self._vms[1][id]['type']
            if type == VMType.MASTER:
                master_exists = True
                self._master_ip = self._vms[1][id]['ip']
                self._master_id = id
            self._cmds[id] = (self._time_started, None)
            self._start_times[id] = self._time_started

        if not master_exists:
            self._master_id = self.create_vm(VMType.MASTER)
            self._master_ip = self._vms[1][self._master_id]['ip']




    def _next_command(self, id, timestamp):
        state = self._vms[1][id]['state']
        load = None
        next_command = None

        if state == VMState.INIT or state == VMState.PENDING:
            next_command = self._pending(id, timestamp)
        elif state == VMState.ACTIVE:
            next_command, load = self._active(id, timestamp)
        elif state == VMState.FAILED:
            return (VMCommand(VMCommand.DELETE, id), load)

        return (next_command, load)

    def _handle_master(self, timestamp):
        command_table = {}

        # first, check if master is still running. if it crashed, delete it,
        # create a new master and suspend the others
        master_command = self._next_command(self._master_id, timestamp)[0]
        if isinstance(master_command, VMCommand) and master_command.type == VMCommand.DELETE:
            self._wait_for_master = True
            for id in self._vms[0]:
                if id != self._master_id and self._vms[1][id]['state'] != VMState.SUSPENDED:
                    command_table[id]= VMCommand(VMCommand.SUSPEND, id)

            command_table[self._master_id] = master_command

            self._master_id = self.create_vm(VMType.MASTER)
            self._master_ip = self._vms[1][self._master_id]['ip']

            self._exec_commands(command_table)
            return True

        # if the master is running again but crashed before: resume the others
        if self._wait_for_master:
            hbt,hb = self._hb_buffer[self._master_id][-1] if self._master_id in self._hb_buffer else (None,None)

            if self._vms[1][self._master_id]['state'] == VMState.ACTIVE and hb and hb.state.state == AppState.RUNNING:
                self._num_workers = 0
                worker_exists = False
                for id in self._vms[0]:
                    
                    type = self._vms[1][id]['type']
                    if type == VMType.WORKER:
                        self._num_workers += 1
                        if not worker_exists:
                            worker_exists = True
                            self._worker_id = id
                    cmdt,cmd = self._cmds[id]

                    # Resume VMs previously suspended due to master restart
                    # (does not execute on initial startup).
                    if isinstance(cmd, VMCommand) and cmd.type == VMCommand.SUSPEND:
                        command_table[id] = VMCommand(VMCommand.RESUME, id)

                if not worker_exists:
                    self._log.info("Creating initial worker, no workers present.")
                    self._worker_id = self.create_vm(VMType.WORKER)


                self._wait_for_master = False
                self._exec_commands(command_table)
            else:
                self._log.info("Waiting for master to be fully operational")
            
            # Still execute the master command (or it never will transition to
            # AppState.RUNNING).
            self._exec_commands({self._master_id : master_command})
            return True

        return False


    def _decide(self, timestamp):
        """Decide on what should happen and execute the command with
        exec_commands"""

        def _is_vm_delete(cmd):
            return isinstance(cmd, VMCommand) and cmd.type == VMCommand.DELETE

        if self._handle_master(timestamp):
            return

        command_table = {}
        total_load = 0.0
        total_count = 0

        for id in self._vms[0]:
            command_table[id], load = self._next_command(id, timestamp)

            if load is not None:
                total_load += load
                total_count += 1




        worker_exists = False
        active_workers = []
        suspended_workers = []
        total_worker_time = 0
        total_worker_threads = 0
        starting_workers = 0
        for id in self._vms[0]:
            type = self._vms[1][id]['type']
            state = self._vms[1][id]['state']

            if type == VMType.WORKER:
                if not worker_exists:
                    worker_exists = True
                    self._worker_id = id

                cmd = command_table[id]
                if state == VMState.ACTIVE and not _is_vm_delete(cmd):
                    hbt,hb = self._hb_buffer[id][-1] if id in self._hb_buffer else (None,None)
                    if hb and hb.state.state == AppState.RUNNING:
                        active_workers.append(id)
                        total_worker_time += hb.avg_time
                        total_worker_threads += hb.workers

                elif state == VMState.SUSPENDED and not _is_vm_delete(cmd):
                    suspended_workers.append(id)

                # If previous command sent is resume or create, assume the
                # worker has just been started.
                prev_cmd = self._cmds[id][1]
                if isinstance(prev_cmd, VMCommand) and (prev_cmd.type == VMCommand.RESUME or prev_cmd.type == VMCommand.CREATE):
                    starting_workers += 1

        if not worker_exists:
            self._worker_id = self.create_vm(VMType.WORKER)
            

        active_worker_count = len(active_workers)
        master_hb_count = len(self._hb_buffer[self._master_id])

        # Scale up or down depending on desired time queue should be empty in
        # (e.g. the queue should be empty in 20 seconds if nothing new was added
        # from now).
        if starting_workers == 0 and active_worker_count > 0 and master_hb_count >= conf.MIN_MEASURE_LOAD_HB: 
            # we may scale again
            #avg_load = total_load / float(total_count) if total_count > 0 else 0.0
            avg_time = float(total_worker_time) / float(active_worker_count)
            total_queue_len = 0
            for hb_tuple in self._hb_buffer[self._master_id][-conf.MIN_MEASURE_LOAD_HB:]:
                total_queue_len += hb_tuple[1].queue_length
            avg_queue_len = float(total_queue_len) / float(conf.MIN_MEASURE_LOAD_HB)
                
            time_left = avg_queue_len / float(total_worker_threads) * avg_time
            self._log.info("Timestamp: %d, Actual time: %f, Avg queue len: %f, Avg time: %f, Threads:  %d, threshold: %f-%f" %(timestamp - self._time_started, time_left, avg_queue_len, avg_time, total_worker_threads, conf.MIN_AVG_TIME, conf.MAX_AVG_TIME))


            if time_left < conf.MIN_AVG_TIME:
                suspend_count = 0
                desired_time = conf.MIN_AVG_TIME + float(conf.MAX_AVG_TIME - conf.MIN_AVG_TIME) / 4
                if time_left == 0.0:
                    suspend_count = active_worker_count - 1
                else:
                    suspend_count = int(math.floor((1 - 1 / (desired_time/time_left)) * active_worker_count))

                self._log.info("Suspending %d VMs (desired time %fs)" % (suspend_count,desired_time))

                for id in reversed(active_workers):
                    if suspend_count:
                        command_table[id] = AppCommand(AppCommand.SHUTDOWN,id)
                        suspend_count -= 1


                # scale down
            elif time_left > conf.MAX_AVG_TIME:
                desired_time = conf.MAX_AVG_TIME - float(conf.MAX_AVG_TIME - conf.MIN_AVG_TIME) / 4
                spin_up_count = int(math.ceil((time_left / desired_time) *
                        active_worker_count - active_worker_count))

                self._log.info("Starting %d VMs (desired time %fs)" % (spin_up_count,desired_time))

                # scale up
                for id in suspended_workers:
                    if spin_up_count:
                        command_table[id] = VMCommand(VMCommand.RESUME,id)
                        spin_up_count -= 1

                for i in range(spin_up_count):
                    if self.create_vm(VMType.WORKER) is None:
                        break

        self._exec_commands(command_table)


    def _exec_commands(self, command_table):

        for id in command_table:
            cmd = command_table[id]
            if cmd is None: 
                continue
                
            executed = False
            deleted = False
            if isinstance(cmd, VMCommand):
                if cmd.type == VMCommand.DELETE:
                    if self._vms[1][id]['type'] == VMType.WORKER:
                        self._num_workers -= 1
                    deleted = True
                    self._log.info("VM %s ran for %d seconds (delete)" % (str(id), int(time.time()) - self._start_times[id]))
                    del self._start_times[id]
                elif cmd.type == VMCommand.SUSPEND:
                    # Remove old heart beats from buffer.
                    del self._hb_buffer[id]
                    self._log.info("VM %s ran for %d seconds (suspend)" % (str(id), int(time.time()) - self._start_times[id]))
                    del self._start_times[id]
                elif cmd.type == VMCommand.RESUME:
                    self._start_times[id] = int(time.time())


                self._wrapper.exec_cmd(cmd)
                executed = True
            elif isinstance(cmd, AppCommand):
                # Send app command.
                self._sock.sendto(pickle.dumps(cmd, pickle.HIGHEST_PROTOCOL),(self._vms[1][id]['ip'], conf.CONTROLLER_PORT))
                executed = True

            if executed:
                self._log.info("Executing %s for %s" % (cmd,str(id)))
                prev_cmd = self._cmds[id][1]
                # If not resent cmd, mark it to be the last sent cmd.
                if deleted:
                    self._log.info("Removing %s from cmd list" % str(id))
                    del self._cmds[id]
                    self._vms = self._wrapper.list_vms(True)
                elif prev_cmd is None or type(prev_cmd) != type(cmd) or prev_cmd.type != cmd.type:
                    self._log.info("Adding  %s to cmd list of %s" % (cmd,str(id)))
                    self._cmds[id] = (time.time(), cmd)


    def create_vm(self, type_enum):
        if type_enum == VMType.WORKER and self._num_workers >= conf.MAX_WORKERS:
            self._log.info("Not creating more workers, at worker limit")
            return None
        self._log.info("Creating VM of type " + VMType.to_str(type_enum))
        cmd = VMCommand(VMCommand.CREATE,type_enum)
        id = self._wrapper.exec_cmd(cmd)
        self._cmds[id] = (time.time(), cmd)
        self._vms = self._wrapper.list_vms()
        self._start_times[id] = int(time.time())
        if type_enum == VMType.WORKER:
            self._num_workers += 1
        return id
            


    def _pending(self, id, timestamp):
        cmdt,cmd = self._cmds[id]
        if timestamp - cmdt > conf.MAX_STARTUP_TIME:
            self._log.error("VM %s takes too long to start" % str(id))
            return VMCommand(VMCommand.DELETE, id)
        return None

            
    def _active(self, id, timestamp):
        hbt,hb = self._hb_buffer[id][-1] if id in self._hb_buffer else (None,None)
        cmdt,cmd = self._cmds[id]

        def _is_vm_create(cmd):
            return isinstance(cmd, VMCommand) and cmd.type == VMCommand.CREATE

        def _is_vm_suspend(cmd):
            return isinstance(cmd, VMCommand) and cmd.type == VMCommand.SUSPEND

        def _is_vm_resume(cmd):
            return isinstance(cmd, VMCommand) and cmd.type == VMCommand.RESUME

        def _is_app_init(cmd):
            return isinstance(cmd, AppCommand) and cmd.type == AppCommand.INIT
            
        def _is_app_shutdown(cmd):
            return isinstance(cmd, AppCommand) and cmd.type == AppCommand.SHUTDOWN

        # 1. No heartbeat ever received -> wait for at most MAX_STARTUP_TIME for
        # first heart beat.
        if not hb:
            return (self._pending(id,timestamp), None)


        app_state = hb.state.state

        # 2. Heart beat received but suspend cmd sent, ignore heart beat.
        if _is_vm_suspend(cmd):
            return (None,None)
            

        # 3. Has heart beat, but out of date (no new one received)
        # (check for recv_time, because old packets are sent after a VM has
        # resumed).
        if timestamp - hb.recv_time > conf.DECISION_INTERVAL:
            self._log.error("VM %s out of date heart beat (%d,%d)." %
            (str(id),timestamp,hb.recv_time))
            return (VMCommand(VMCommand.DELETE, id), None)

        # 4. Heart beat and not out of date.
        if app_state == AppState.ERROR:
            # Try to resolve the error by sending an INIT if in time.
            if _is_app_init(cmd) and timestamp - cmdt > conf.MAX_TRANSITION_TIME:
                self._log.error("VM %s not recovering from error in time." % str(id))
                return (VMCommand(VMCommand.DELETE, id), None)
            return self._create_app_command_init(id)

        if _is_vm_create(cmd) and app_state == AppState.READY:
            return self._create_app_command_init(id)

        if _is_app_init(cmd) and (app_state == AppState.READY or app_state == AppState.INIT):
            if timestamp - cmdt > conf.MAX_TRANSITION_TIME:
                self._log.error("VM %s not transitioning in time." % str(id))
                return (VMCommand(VMCommand.DELETE, id), None)
            return self._create_app_command_init(id)


        if _is_app_init(cmd) and app_state == AppState.RUNNING:
            load = None
            buffer_len = len(self._hb_buffer[id])
            if self._vms[1][id]['type'] == VMType.WORKER and buffer_len >= conf.MIN_MEASURE_LOAD_HB:
                # Second check is in place because just resumed or created nodes
                # tend to have a very high cpu load (2.0 or higher), and the
                # controller should not create more nodes because of this bias.
                if hb.mem > conf.MAX_AVG_LOAD or hb.disk > conf.MAX_AVG_LOAD:
                    load = max(hb.cpu,hb.mem,hb.disk)
                else:
                    load = hb.cpu

            return (None, load)

        if _is_app_shutdown(cmd) and (app_state == AppState.RUNNING or app_state == AppState.SHUTTING_DOWN):
            if timestamp - cmdt > conf.MAX_SHUTDOWN_TIME:
                self._log.error("VM %s not transitioning in time." % str(id))
                return (VMCommand(VMCommand.DELETE, id), None)
            return (AppCommand(AppCommand.SHUTDOWN, id), None)

        if _is_app_shutdown(cmd) and app_state == AppState.READY:
            return (VMCommand(VMCommand.SUSPEND, id), None)

        # This only occurs when the controller just restarted.
        # Adapt to the heartbeat received.
        if cmd is None or _is_vm_resume(cmd):
            if app_state == AppState.RUNNING or app_state == AppState.READY or app_state == AppState.INIT:
                return self._create_app_command_init(id)
            if app_state == AppState.SHUTTING_DOWN:
                return (AppCommand(AppCommand.SHUTDOWN, id), None)
                

        self._log.error("VM %s is in illegal state" % str(id))
        return (VMCommand(VMCommand.DELETE, id), None)



    def run(self):
    
        try:
            while True:
                time.sleep(conf.DECISION_INTERVAL)
                timestamp = int(time.time())

                # ( [id,id2], {id -> {ip->,type->,state->}},{ip->id,ip2->id2})
                self._vms = self._wrapper.list_vms(True)


                # { (ip,port) -> queue , ip2 -> queue }
                hb_buffer = self._monitor.flush()

                for addr in hb_buffer:
                    ip = addr[0]
                    if not ip in self._vms[2]:
                        continue

                    id = self._vms[2][ip]
                    if not id in self._hb_buffer:
                        self._hb_buffer[id] = []
                    self._hb_buffer[id] = self._hb_buffer[id] + list(hb_buffer[addr].queue)

                    if len(self._hb_buffer[id]) > conf.MAX_HB_HISTORY:
                        self._hb_buffer[id] = self._hb_buffer[id][-conf.MAX_HB_HISTORY:]

                self._decide(timestamp)
        except KeyboardInterrupt:
            timestamp = int(time.time())
            for id in self._start_times:
                self._log.info("VM %s ran for %d seconds (interrupt)" % (str(id), timestamp - self._start_times[id]))
            raise




    def _create_app_command_init(self, id):
        return (AppCommand(AppCommand.INIT, self._master_ip), None)




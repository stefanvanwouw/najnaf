import subprocess, re, conf, os
from shared import Command


class VMType(object):
    MASTER = 0
    WORKER = 1
    PRODUCER = 2

    @staticmethod
    def to_str(type):
        if type == VMType.MASTER:
            return 'MASTER'
        elif type == VMType.WORKER:
            return 'WORKER'
        return 'PRODUCER'


    @staticmethod
    def from_str(str):
        str = str.upper()
        if str == 'MASTER':
            return VMType.MASTER
        elif str == 'WORKER':
            return VMType.WORKER
        return VMType.PRODUCER

class VMState(object):
    INIT = 0
    PENDING = 1
    HOLD = 2
    ACTIVE = 3
    STOPPED = 4
    SUSPENDED = 5
    DONE = 6
    FAILED = 7
    UNKNOWN = 8


    @staticmethod
    def to_str(state):
        return {VMState.INIT : 'INIT',
                VMState.PENDING : 'PENDING',
                VMState.HOLD : 'HOLD',
                VMState.ACTIVE : 'ACTIVE',
                VMState.STOPPED : 'STOPPED',
                VMState.SUSPENDED : 'SUSPENDED',
                VMState.DONE : 'DONE',
                VMState.FAILED : 'FAILED',
                VMState.UNKNOWN : 'UNKNOWN'
        }[state]

    @staticmethod
    def from_str(str):
        str = str.upper()
        try:
            return {'INIT': VMState.INIT,
                    'PENDING': VMState.PENDING,
                    'HOLD': VMState.HOLD,
                    'ACTIVE': VMState.ACTIVE,
                    'STOPPED': VMState.STOPPED,
                    'SUSPENDED': VMState.SUSPENDED,
                    'DONE': VMState.DONE,
                    'FAILED': VMState.FAILED,
            }[str]
        except KeyError:
            return VMState.UNKNOWN

    


class OneVMWrapper(object):
    """Mapping between address based identification and VM_ID based
    identification. Also provides a way to suspend or start new VMs in
    OpenNebula. Assumes migration never occurs by an external system."""

    # TODO: Make use of the official open nebula API instead of onevm command. 
    # This is the only DAS-4 specific code in the entire project. Adapt this class to Amazon EC2 for example in order to run the system on Amazon EC 2.


    def __init__(self):
        self._load_vm_mappings() 
        
        
    def _load_vm_mappings(self):
        self._mapping = {}
        self._id_ordering = []
        self._remapping = {}
        output = subprocess.Popen(["onevm", "list"], stdout=subprocess.PIPE).communicate()
        lines = output[0].split('\n')[1:-1]
        for line in lines:
            split = line.split()
            if len(split) == 0:
                continue
            id = split[0]
            type = VMType.from_str(split[3])
            ip,state = self._get_vm_ip_and_state(id)
            self._id_ordering.append(id)
            self._mapping[id] = {'ip':ip, 'type': type, 'state': state}
            self._remapping[ip] = id

    def _get_vm_ip_and_state(self, id):
        ip_reg = re.compile(r"IP=\"(.+?)\"", re.MULTILINE)
        state_reg = re.compile(r"STATE\s*:\s*(\w+)", re.MULTILINE)
        out = subprocess.Popen(["onevm","show",id], stdout=subprocess.PIPE).communicate()
        return (re.search(ip_reg, out[0]).group(1),
                VMState.from_str(re.search(state_reg, out[0]).group(1)))



    def list_vms(self, load=False):
        if load:
            self._load_vm_mappings()
        return (list(self._id_ordering),self._mapping.copy(), self._remapping.copy())



    def _create_vm(self, type_enum):
        """Create a VM, and register it in the wrapper."""
        one = VMType.to_str(type_enum).lower() + ".one"
        output = subprocess.Popen(["onevm", "create", os.path.expanduser(conf.VM_IMAGE_DIR)+one], stdout=subprocess.PIPE).communicate()
        id_reg = re.compile(r"ID:\s*(\d+)", re.MULTILINE)
        id = re.search(id_reg, output[0]).group(1)
        ip,state = self._get_vm_ip_and_state(id)
        self._mapping[id] = {'ip': ip, 'type': type_enum, 'state':state}
        self._id_ordering.append(id)
        self._remapping[ip] = id
        return id



    def _suspend_vm(self, id):
        if id not in self._mapping:
            raise VMError("VM does not exist.")
        subprocess.Popen(["onevm", "suspend", id])

    def _delete_vm(self, id):
        if id not in self._mapping:
            raise VMError("VM does not exist.")
        subprocess.Popen(["onevm", "delete", id])
        del self._remapping[self._mapping[id]['ip']]
        del self._mapping[id]
        self._id_ordering.remove(id)

    def _resume_vm(self, id):
        if id not in self._mapping:
            raise VMError("VM does not exist.")
        subprocess.Popen(["onevm", "resume", id])

    def exec_cmd(self, cmd):
        if not isinstance(cmd, VMCommand):
            return
        if cmd.type == VMCommand.CREATE:
            return self._create_vm(cmd.data)
        if cmd.type == VMCommand.SUSPEND:
            self._suspend_vm(cmd.data)
            return
        if cmd.type == VMCommand.DELETE:
            self._delete_vm(cmd.data)
            return
        if cmd.type == VMCommand.RESUME:
            self._resume_vm(cmd.data)
            return


class VMCommand(Command):
    CREATE = 0
    SUSPEND = 1
    DELETE = 2
    RESUME = 3
    

    def __str__(self):
        return "(%s,%s)" % ({self.CREATE: 'CREATE',
                self.DELETE: 'DELETE',
                self.RESUME: 'RESUME',
                self.SUSPEND: 'SUSPEND'}[self._type], str(self._data))

class VMError(Exception):
    pass

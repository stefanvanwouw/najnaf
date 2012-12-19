import conf, socket, sys, pickle
from shared import AppCommand

# Utility to simulate head node commands

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

cmd = AppCommand(int(sys.argv[2]), sys.argv[3])
print cmd
sock.sendto(pickle.dumps(cmd, pickle.HIGHEST_PROTOCOL),(sys.argv[1], conf.CONTROLLER_PORT))

import stomp
import conf
import time
import sys

ip = sys.argv[1]

conn = stomp.Connection(host_and_ports =
    [ (ip, 61613) ],
    user=conf.APOLLO_USER,
    passcode=conf.APOLLO_PASSWORD
)
conn.start()
conn.connect()

count = int(sys.argv[2])
interval = int(sys.argv[3])
busy_wait = sys.argv[4]

while True:
    def send(message):
        conn.send(message, destination='/queue/test')
        return True

    for i in range(count):
        if not send(busy_wait):
            print 'error: sending failed'
            sys.exit(1)

    time.sleep(interval)


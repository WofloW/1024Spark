#!/usr/bin/env python
#python pyremote.py localhost start_worker.sh

import os
import os.path
import subprocess
import sys
import gevent

class Remote(object):
    
    def __init__(self, host, command, master_ipport, worker_port):
        self.host    = host
        self.curdir  = os.getcwd()
        self.remote_command = os.path.join(self.curdir, command)
        self.master_ipport = master_ipport
        self.worker_port = worker_port
        
    def run(self):
        proc = subprocess.call(['ssh', self.host, self.remote_command, self.curdir, self.master_ipport, self.worker_port])

if __name__ == '__main__':
    driver_ip = sys.argv[1]
    command = "start_worker.sh"
    master_ipport = sys.argv[2]
    worker_port = sys.argv[3]
    remote = Remote(driver_ip, command, master_ipport, worker_port)
    remote.run()
    # while True:
    #     print 1234
    #     gevent.sleep(1)

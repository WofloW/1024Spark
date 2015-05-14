#!/usr/bin/env python
#python pyremote.py localhost start_worker.sh

import os
import os.path
import subprocess
import sys
import gevent

if __name__ == '__main__':
    # driver_ip = sys.argv[1]
    # command = "start_worker.sh"
    # master_ipport = sys.argv[2]
    # worker_port = sys.argv[3]
    subprocess.Popen(["python", "pyremote.py",  "127.0.0.1", "127.0.0.1:4000", "4002"])
    subprocess.Popen(["python", "pyremote.py",  "127.0.0.1", "127.0.0.1:4000", "4003"])
    while True:
        print 1234
        gevent.sleep(1)

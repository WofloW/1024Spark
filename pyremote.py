#!/usr/bin/env python
#python pyremote.py localhost start_worker.sh

import os
import os.path
import subprocess
import sys

class Remote(object):
    
    def __init__(self, host, command):
        self.host    = host
        self.command = command
        self.curdir  = os.getcwd()
        self.remote_command = os.path.join(self.curdir, self.command)


    def run(self):
        proc = subprocess.call(['ssh', self.host, self.remote_command])

if __name__ == '__main__':
    remote = Remote(sys.argv[1], sys.argv[2])
    remote.run()
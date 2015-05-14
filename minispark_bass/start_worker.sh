#!/bin/bash
#1 working_dir, 2 master_ipport, 3 worker_port
cd $1
/opt/python2.7/bin/python2.7 ms_Worker.py $2 $3

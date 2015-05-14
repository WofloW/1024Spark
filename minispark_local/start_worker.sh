#!/bin/bash
#1 working_dir, 2 master_ipport, 3 worker_port
cd $1
python ms_Worker.py $2 $3

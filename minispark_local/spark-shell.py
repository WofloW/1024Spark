import sys
import code
from gevent import fileobject
from ms_SparkContext import *

# Usage:
# python spark-shell.py 4000

'''
    Micro Spark Shell
'''

_green_stdin = fileobject.FileObject(sys.stdin)
_green_stdout = fileobject.FileObject(sys.stdout)


def _green_raw_input(prompt):
    _green_stdout.write(prompt)
    return _green_stdin.readline()[:-1]


def run_console(local=None, prompt=">>>"):
    code.interact(prompt, _green_raw_input, local=local or {})


if __name__ == '__main__':
    subprocess.call(["sh", "stopall.sh"])
    port = sys.argv[1]
    spark = SparkContext()
    gevent.joinall([
        gevent.spawn(spark.createServerHandle, port),
        gevent.spawn(run_console, {"spark": spark})
    ])
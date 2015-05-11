import sys
import code
from gevent import fileobject

_green_stdin = fileobject.FileObject(sys.stdin)
_green_stdout = fileobject.FileObject(sys.stdout)

def _green_raw_input(prompt):
    _green_stdout.write(prompt)
    return _green_stdin.readline()[:-1]

def run_console(local=None, prompt=">>>"):
    code.interact(prompt, _green_raw_input, local=local or {})

if __name__ == "__main__":
    run_console()
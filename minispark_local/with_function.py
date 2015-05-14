import pickle
import marshal
from types import FunctionType, CodeType

# Hack into pickle module and enable function serialization
# This operation has a global effect, but it's not a big deal because 
# all pickle module used in this project needs function serialization.
pickle.FUNCTION = 'f'


def get_glob_name(code):
    for o in code.co_consts:
        if type(o) is CodeType:
            for n in get_glob_name(o):
                yield n
    for n in code.co_names:
        yield n


def save_func(pickler, func):
    # prepare serializable function closure
    if func.func_closure is None:
        closure = None
    else:
        closure = tuple(c.cell_contents for c in func.func_closure)

    # pick used global values
    glob = {}
    for g_var in get_glob_name(func.func_code):
        if g_var not in glob:
            if g_var in func.func_globals:
                glob[g_var] = func.func_globals[g_var]
            else:
                # try:
                #    glob['__builtins__'][g_var] = func.func_globals['__builtins__'][g_var]
                #except:
                #    try:
                #        glob[g_var] = getattr(__builtin__, g_var)
                #    except:
                pass

    # write to stream
    pickler.save((marshal.dumps(func.func_code),
                  glob,
                  func.func_name,
                  func.func_defaults,
                  closure))
    pickler.write(pickle.FUNCTION)
    pickler.memoize(func)


pickle.Pickler.dispatch[FunctionType] = save_func


def load_func(unpickler):
    def Cell(content):
        return (lambda: content).func_closure[0]

    code, glob, name, argdefs, closure = unpickler.stack[-1]

    code = marshal.loads(code)
    if closure is not None:
        closure = tuple(Cell(c) for c in closure)

    glob['__builtins__'] = globals()['__builtins__']

    unpickler.stack[-1] \
        = FunctionType(code=code, globals=glob, name=name, argdefs=argdefs, closure=closure)


pickle.Unpickler.dispatch[pickle.FUNCTION] = load_func
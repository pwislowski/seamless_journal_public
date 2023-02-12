# Logger to support websockets
# Prevent from sending multiple requests for the same position to `notiondb`
from os import mkdir

def create_logdir(dir_name:str = 'logs'):
    try:
        mkdir(dir_name)
    
    except FileExistsError:
        pass


def logger(func):
    def wrapper(*args, **kwargs):
        val = func(*args, **kwargs)

        create_logdir()
        
        fname = kwargs['fname']
        with open(f'./logs/{fname}.txt', 'w') as f:
            f.write(str(val))
            
        return val
    return wrapper


def trade_comparer(inp:dict, fname:str) -> bool:
    """Check if trade has already been logged in."""
    
    try:
        with open(f'./logs/{fname}.txt', 'r') as f:
            logged = f.read()
            logged = eval(logged)
    except FileNotFoundError:
        return False    
    
    for key in inp.keys():
        try:
            if inp[key] != logged[key]:
                return False
        
        except KeyError:
            return False
        
    return True

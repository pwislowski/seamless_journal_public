from __future__ import annotations

from pathlib import Path
import subprocess
from concurrent.futures import ProcessPoolExecutor

def get_files() -> list[Path]:
    p = Path('.').glob('*.py')
    
    return [i for i in p if i.name != 'script.py']

def spawn_process(file:str) -> None:
    subprocess.Popen(['python3', f'./{file}'], universal_newlines= True)

    return 1

if __name__ == '__main__':
    files = [F.name for F in get_files()]
    with ProcessPoolExecutor() as executor:
        fut = [executor.submit(spawn_process, F) for F in files]
    
    print('iterated')

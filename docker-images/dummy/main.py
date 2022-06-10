import os
import time
import random

from aisprint.annotations import exec_time, component_name, annotation

@annotation({'component_name': {'name': 'C1'}, 'exec_time': {'local_time_thr': 500}})
def main(args):
    # print("Hello! I'm an example component C1!")
    r = random.uniform(5, 15)
    print(str(r))
    time.sleep(r)

if __name__ == '__main__':
    main({})

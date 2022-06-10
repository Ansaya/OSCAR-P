import os

from aisprint.annotations import exec_time, component_name, annotation

@annotation({'component_name': {'name': 'C1'}, 'exec_time': {'local_time_thr': 500}})
def main(args):
    print("Hello! I'm an example component C1!")

if __name__ == '__main__':
    main({})
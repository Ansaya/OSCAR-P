import os

from aisprint.annotations import exec_time, component_name

@component_name(name='C2')
@exec_time(local_time_thr=200, global_time_thr=1000, prev_components=['C1'])
def main(args):
    print("Hello! I'm an example component C2!")

if __name__ == '__main__':
    main({})

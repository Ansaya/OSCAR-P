# collection of utils for CLI, GUI and SSH

import subprocess
import time

from paramiko import SSHClient, AutoAddPolicy
from termcolor import colored


# CLI utils

# todo should replace all usage with get_command_output_wrapped where error control is needed (which should be always)
def execute_command(command):
    p = subprocess.Popen(command.split())
    p.wait()
    return


# execute a command externally, returns the output lines and errors
# todo should always use the wrapped version
def get_command_output(command):
    output = subprocess.Popen(command.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    lines = []
    for line in output.stdout.readlines():
        line = line.decode('utf-8')
        lines.append(line)

    errors = []
    for e in output.stderr.readlines():
        e = e.decode('utf-8')
        errors.append(e)

    return lines, errors


def get_command_output_wrapped(command):
    for t in [5, 15, 30]:
        lines, errors = get_command_output(command)
        if errors:  # empty list equals to False
            show_warning("Errors encountered, retrying in " + str(t) + " seconds")
            time.sleep(t)
        else:
            return lines
    show_error("Errors encountered, cannot proceed. Exiting.")
    quit()


def show_warning(message):
    print(colored("Warning: " + message, "yellow"))


def show_error(message):
    print(colored("Error: " + message, "red"))


# GUI utils

def get_valid_input(message, allowed_values):
    value = input("\n" + message)
    while value not in allowed_values:
        print("Answer not valid")
        value = input("\n" + message)
    return value


# SSH utils

def configure_ssh_client():
    from input_file_processing import get_cluster_ssh_info

    address, port, username = get_cluster_ssh_info()
    client = SSHClient()
    client.set_missing_host_key_policy(AutoAddPolicy())
    client.connect(address, port=port, username=username)
    return client


# returns output of a command executed via ssh as a list of lines
def get_ssh_output(client, command):
    stdin, stdout, stderr = client.exec_command(command)
    lines = stdout.readlines()
    if not lines:  # if empty
        lines = stderr.readlines()
    stdin.close()
    stdout.close()
    stderr.close()
    return lines

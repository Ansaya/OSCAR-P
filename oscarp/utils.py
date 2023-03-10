# collection of utils for CLI, GUI and SSH
import os
import subprocess
import time
import json
import csv
import shutil
import re

import yaml
from paramiko import SSHClient, AutoAddPolicy, RSAKey
from termcolor import colored

import executables


# CLI utils

# execute a command externally, returns the output lines and errors
def _get_command_output(command):
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
    for t in [5, 5, 10, 15, 30, 60, 120, 5*60, 10*60]:
        lines, errors = _get_command_output(command)
        if errors:  # empty list equals to False
            show_warning("Errors encountered, retrying in " + str(t) + " seconds")
            for e in errors:
                show_external_error(e)
            time.sleep(t)
        else:
            return lines
    show_error("Errors encountered, cannot proceed. Exiting.")
    quit()


def show_external_error(message):
    print(colored(message, "red"))


def show_warning(message):
    print(colored("\nWarning: " + message, "yellow"))


def show_error(message):
    print(colored("\nError: " + message, "red"))


def show_fatal_error(message):
    """
    prints an unrecoverable error message and exits
    :param message: error message
    """

    show_error(message)
    quit()


def show_debug_info(message):
    from input_file_processing import get_debug

    if get_debug():
        print(colored("\nInfo: " + message, "cyan"))


def get_valid_input(message, allowed_values):
    value = input("\n" + message)
    while value not in allowed_values:
        print("Answer not valid")
        value = input("\n" + message)
    return value


# SSH utils

def configure_ssh_client(cluster):
    username, ip_address = cluster["ssh"].split("@")
    address, port = ip_address.split("/")
    # print(address, port, username)
    client = SSHClient()
    client.set_missing_host_key_policy(AutoAddPolicy())
    private_key = get_private_key()
    client.connect(address, port=port, username=username, pkey=private_key)
    return client


def get_private_key():
    with open(executables.ssh, "r") as file:
        data = json.load(file)

    private_key = RSAKey.from_private_key_file(executables.ssh_key, data["password"])
    # private_key = RSAKey.from_private_key_file(data["path_to_key"], data["password"])
    return private_key


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


# FILE UTILS

def write_list_of_strings_to_file(list_of_strings, filepath):
    with open(filepath, "w") as file:
        for s in list_of_strings:
            file.write(s + "\n")


def append_string_to_file(string, filepath):
    with open(filepath, "a") as file:
        file.write(string + "\n")


def create_new_file(filepath):
    with open(filepath, "w"):
        pass


def auto_mkdir(new_dir):
    if not os.path.exists(new_dir):
        os.mkdir(new_dir)


def csv_to_list_of_dict(filepath):
    with open(filepath, mode='r') as file:
        reader = csv.DictReader(file)
        line_count = 1
        list_of_dict = []
        for row in reader:
            list_of_dict.append(row)
            line_count += 1
    return list_of_dict
    

def delete_directory(dir_path):
    shutil.rmtree(dir_path)


def delete_file(file_path):
    os.remove(file_path)


def ensure_slash_end(path):
    if path[-1] != "/":
        path += "/"
    return path


def read_json(path):
    if not os.path.exists(path):
        return {}

    with open(path, 'r') as file:
        return json.load(file)


def write_json(path, output):
    with open(path, "w+") as file:
        json.dump(output, file, indent=4)


def read_yaml(path):
    if not os.path.exists(path):
        return {}

    with open(path) as file:
        return yaml.load(file, Loader=yaml.FullLoader)


def write_yaml(path, output):
    with open(path, "w+") as file:
        yaml.dump(output, file, sort_keys=False)


# OTHER UTILS

def dict_to_string(input_dict):
    return json.dumps(input_dict)


def make_debug_info(raw_list):
    # print(raw_list)
    output_line = ""

    for r in raw_list:
        if isinstance(r, str):
            output_line += r + "\n"
        elif isinstance(r, dict):
            output_line += dict_to_string(r) + "\n"
        else:
            show_error("Whoopsie, unexpected type")

    # print(output_line)
    return output_line


def strip_ansi_from_string(string):
    ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
    return ansi_escape.sub('', string)


"""
def addition_lists(list_one, list_two):
    diff = len(list_one) - len(list_two)

    if diff > 0:  # list_one is bigger
        for i in range(diff):
            list_two.append(0)
    else:
        for i in range(abs(diff)):  # list two is bigger (or they are equal and nothing happens)
            list_one.append(0)

    result = []
    for i in range(len(list_one)):
        result.append(list_one[i] + list_two[i])

    return result
"""

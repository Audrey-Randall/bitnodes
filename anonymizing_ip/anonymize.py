import re
import hashlib
import uuid
import fileinput
import sys
import pickle
import bz2
from pathlib import Path


salt = uuid.uuid4().hex


def serialize_set(obj):
    if isinstance(obj, set):
        return list(obj)
    return obj


def replace_json(s):
    hash = hashlib.sha256(salt.encode('utf8') + s.group(0).encode("utf-8"))
    hash = hash.hexdigest()  # + ":" + salt
    # print(s.group(0), file=sys.stderr)
    # print(hash, file=sys.stderr)
    # print(s.group(1), s.group(2), s.group(3), file=sys.stderr)
    return "[\"{}\",\"{}.{}.{}.0\"]".format(hash, s.group(1), s.group(2), s.group(3))


def replace_csv(s):
    hash = hashlib.sha256(salt.encode('utf8') + s.group(0).encode("utf-8"))
    hash = hash.hexdigest()  # + ":" + salt
    # print(s.group(0), file=sys.stderr)
    # print(hash, file=sys.stderr)
    # print(s.group(1), s.group(2), s.group(3), file=sys.stderr)
    return "{}-{}.{}.{}.0".format(hash, s.group(1), s.group(2), s.group(3))


def substitute(filename, regex, replace):
    for line in fileinput.input(filename, inplace=True):
        new = re.sub(regex, replace, line)
        # print(new, file=sys.stderr)
        sys.stdout.write(new)


def anonymize_node(node):
    split = node.split("-")
    rightmost_content = "-".join(split[1:])
    new_ip = split[0]
    hash = hashlib.sha256(salt.encode('utf8') + new_ip.encode("utf8"))
    hash = hash.hexdigest()
    if "." in new_ip:
        new_ip = new_ip.split(".")
        new_ip[-1] = "0"
        new_ip = ".".join(new_ip)
    elif ":" in new_ip:
        new_ip = new_ip.split(":")
        new_ip[-1] = "0"
        new_ip = ":".join(new_ip)
    else:
        print("must not print this")
    return "-".join([hash, new_ip, rightmost_content])


if __name__ == "__main__":

    ip_capture_regex_json = "\"([0-9]{1,3})\.([0-9]{1,3})\.([0-9]{1,3})\.([0-9]{1,3})\""
    ip_capture_regex_csv = "([0-9]{1,3})\.([0-9]{1,3})\.([0-9]{1,3})\.([0-9]{1,3})"
    filenames_json = Path(sys.argv[1]).glob("**/*.json")
    for filename in filenames_json:
        substitute(filename, ip_capture_regex_json, replace_json)
        print(filename)
    filenames_csv = Path(sys.argv[1]).glob("**/*.csv")
    for filename in filenames_csv:
        substitute(filename, ip_capture_regex_csv, replace_csv)
        print(filename)
    filenames_pickle = Path(sys.argv[1]).glob("**/*.pickle.bz2")
    for filename_pickle in filenames_pickle:
        print(filename_pickle)
        with bz2.open(filename_pickle, "rb") as f:
            tmp = pickle.load(f)
        new_data = {}
        for reachable_node, nodes in tmp.items():
            anonymized_reachable_node = anonymize_node(reachable_node)
            new_data[anonymized_reachable_node] = []
            for node in nodes:
                anonymized_node = anonymize_node(node)
                new_data[anonymized_reachable_node].append(anonymized_node)
        with bz2.open(filename_pickle, "wb") as f:
            pickle.dump(new_data, f)

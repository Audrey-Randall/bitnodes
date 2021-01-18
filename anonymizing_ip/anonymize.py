import re
import hashlib
import uuid
import fileinput
import sys
import glob
import os
from pathlib import Path


salt = uuid.uuid4().hex


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


if __name__ == "__main__":

    ip_capture_regex_json = "\"([0-9]{1,3})\.([0-9]{1,3})\.([0-9]{1,3})\.([0-9]{1,3})\""
    ip_capture_regex_csv = "([0-9]{1,3})\.([0-9]{1,3})\.([0-9]{1,3})\.([0-9]{1,3})"
    filenames_json = Path(sys.argv[1]).glob("**/*.json")
    filenames_csv = Path(sys.argv[1]).glob("**/*.csv")
    for filename in filenames_json:
        substitute(filename, ip_capture_regex_json, replace_json)
    for filename in filenames_csv:
        substitute(filename, ip_capture_regex_csv, replace_csv)

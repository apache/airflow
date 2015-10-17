#!/usr/bin/env python
import requirements
import argparse
import os
import re

parser = argparse.ArgumentParser()
parser.add_argument('file', help="requirements.txt", type=str)
parser.add_argument('wheeldir', help="wheeldir location", type=str)

args = parser.parse_args()

req_file = open(args.file, 'r')

names = os.listdir(args.wheeldir)

def wheel_exists(which):
    '''Returns list of filenames from `where` path matched by 'which'
       shell pattern. Matching is case-insensitive.'''

    return filter(lambda x: re.search(which, x, re.IGNORECASE), names)

for req in requirements.parse(req_file):
    print "Checking " + args.wheeldir + os.path.sep + req.name + "*.whl"
    if not wheel_exists(req.name):
        os.system("pip wheel --wheel-dir=" + args.wheeldir + " " + req.name + "".join(req.specs) + "".join(req.extras))

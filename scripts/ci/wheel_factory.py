#!/usr/bin/env python
import requirements
import argparse
import glob
import os
import re
import fnmatch

parser = argparse.ArgumentParser()
parser.add_argument('file', help="requirements.txt", type=str)
parser.add_argument('wheeldir', help="wheeldir location", type=str)

args = parser.parse_args()

req_file = open(args.file, 'r')

def findfiles(which, where='.'):
    '''Returns list of filenames from `where` path matched by 'which'
       shell pattern. Matching is case-insensitive.'''

    # TODO: recursive param with walk() filtering
    rule = re.compile(fnmatch.translate(which), re.IGNORECASE)
    return [name for name in os.listdir(where) if rule.match(name)]

for req in requirements.parse(req_file):
    print "Checking " + args.wheeldir + os.path.sep + req.name + "*.whl"
    if not findfiles(req.name + "*.whl", args.wheeldir):
        os.system("pip wheel --wheel-dir=" + args.wheeldir + " " + req.name + "".join(req.specs) + "".join(req.extras))

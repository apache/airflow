#!/usr/bin/env python
import requirements
import argparse
import glob
import os

parser = argparse.ArgumentParser()
parser.add_argument('file', help="requirements.txt", type=str)
parser.add_argument('wheeldir', help="wheeldir location", type=str)

args = parser.parse_args()

req_file = open(args.file, 'r')

for req in requirements.parse(req_file):
    print "Checking " + args.wheeldir + os.path.pathsep + req.name + "*.whl"
    if not glob.glob(args.wheeldir + os.path.pathsep + req.name + "*.whl"):
        os.system("pip wheel --wheel-dir=" + args.wheeldir + " " + req.name + "".join(req.specs) + "".join(req.extras))

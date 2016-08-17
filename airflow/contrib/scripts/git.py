#!/usr/bin/env python
from __future__ import print_function, absolute_import, division

import logging

from collections import defaultdict
from errno import ENOENT
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time
import subprocess
import sys

import fuse
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

if not hasattr(__builtins__, 'bytes'):
    bytes = str

now = time()

# UTIL

def stdout_of(cmd, cwd):
    proc = subprocess.Popen(
        cmd,
        cwd=cwd,
        stdout=subprocess.PIPE
    )
    out, err = proc.communicate()
    assert err is None

    return out

def commit_hashes_of(git_directory):
    return stdout_of(
        ['git', 'rev-list', '--all'],
        cwd=git_directory
    ).split('\n')

def tree_of_commit(git_directory, commit_hash):
    lines = stdout_of(
        ['git', 'cat-file', '-p', commit_hash],
        cwd=git_directory
    ).split('\n')

    _, tree_hash = lines[0].split(' ')
    return tree_hash

_directory_of_tree_cache = {}
def directory_of_tree(git_directory, tree_hash):

    key = (git_directory, tree_hash)
    if key in _directory_of_tree_cache: return _directory_of_tree_cache[key]

    lines = stdout_of(
        ['git', 'cat-file', '-p', tree_hash],
        cwd=git_directory
    ).split('\n')

    ret = []

    for line in lines:
        if not len(line): continue
        a, filename = line.split('\t')
        b, c, d = a.split(' ')

        ret += [(b, c, d, filename)]

    _directory_of_tree_cache[key] = ret
    return _directory_of_tree_cache[key]

def directory_of_commit(git_directory, commit_hash):
    return directory_of_tree(git_directory, tree_of_commit(git_directory, commit_hash))


_object_hash_of_cache = {}
def object_hash_of(git_directory, tree_hash, path):

    key = (git_directory, tree_hash, tuple(path))
    if key in _object_hash_of_cache: return _object_hash_of_cache[key]

    if len(path) == 0:
        return tree_hash

    match = None
    for _, blob_or_tree, object_hash, filename in directory_of_tree(git_directory, tree_hash):
        if filename == path[0]:
            match = (blob_or_tree, object_hash, filename)

    if match is None:
        raise FuseOSError(ENOENT)

    blob_or_tree, object_hash, filename = match

    _object_hash_of_cache[key] = object_hash_of(git_directory, object_hash, path[1:])
    return _object_hash_of_cache[key]


_object_type_of_cache = {}
def object_type_of(git_directory, object_hash):

    key = (git_directory, object_hash)
    if key in _object_type_of_cache: return _object_type_of_cache[key]

    ret = stdout_of(
        ['git', 'cat-file', '-t', object_hash],
        cwd=git_directory
    ).split('\n')[0]

    _object_type_of_cache[key] = ret
    return _object_type_of_cache[key]

_blob_contents_cache = {}
def get_blob_contents(git_directory, blob_hash):

    key = (git_directory, blob_hash)
    if key in _blob_contents_cache:
        return _blob_contents_cache[key]

    _blob_contents_cache[key] = stdout_of(
        ['git', 'cat-file', '-p', blob_hash],
        cwd=git_directory
    )
    return _blob_contents_cache[key]

# GIT

class Git(LoggingMixIn, Operations):
    'Example memory filesystem. Supports only one level of files.'

    def __init__(self, git_directory):
        self.git_directory = git_directory
        self.commit_hashes = commit_hashes_of(self.git_directory)

        # todo: delete
        self.files = {}
        self.data = defaultdict(bytes)
        self.fd = 0
        now = time()
        self.files['/'] = dict(st_mode=(S_IFDIR | 0o755), st_ctime=now,
                               st_mtime=now, st_atime=now, st_nlink=2)


    def chmod(self, path, mode):
        raise FuseOSError(fuse.EROFS)

    def chown(self, path, uid, gid):
        raise FuseOSError(fuse.EROFS)

    def create(self, path, mode):
        raise FuseOSError(fuse.EROFS)

    def getattr(self, path, fh=None):

        assert path[0] == '/'

        if path == '/' or path[1:] in self.commit_hashes:
            return dict(st_mode=(S_IFDIR | 0o755), st_ctime=now,
                                   st_mtime=now, st_atime=now, st_nlink=2)
        else:
            path_parts = path[1:].split('/')

            if path_parts[0] not in self.commit_hashes:
                raise FuseOSError(ENOENT)
            else:

                commit_sha, path_parts = path_parts[0], path_parts[1:]

                object_hash = object_hash_of(
                    self.git_directory,
                    tree_of_commit(self.git_directory, commit_sha),
                    path_parts
                )

                object_type = object_type_of(self.git_directory, object_hash)

                assert object_type in ['blob', 'tree']

                if object_type == 'tree':
                    return dict(st_mode=(S_IFDIR | 0o755), st_ctime=now,
                                st_mtime=now, st_atime=now, st_nlink=2)
                else:
                    return dict(st_mode=(S_IFREG | 0o755), st_ctime=now,
                                st_mtime=now, st_atime=now, st_nlink=2, st_size=len(get_blob_contents(self.git_directory, object_hash)))



    def getxattr(self, path, name, position=0):
        raise FuseOSError(fuse.EROFS)

    def listxattr(self, path):
        raise FuseOSError(fuse.EROFS)

    def mkdir(self, path, mode):
        raise FuseOSError(fuse.EROFS)

    def open(self, path, flags):
        self.fd += 1
        return self.fd

    def read(self, path, size, offset, fh):

        assert path[0] == '/'

        path_parts = path[1:].split('/')

        commit_hash, path_parts = path_parts[0], path_parts[1:]

        object_hash = object_hash_of(
            self.git_directory,
            tree_of_commit(self.git_directory, commit_hash),
            path_parts
        )

        return get_blob_contents(self.git_directory, object_hash)[offset:offset+size]

    def readdir(self, path, fh):

        if path == '/':
            return self.commit_hashes
        elif path[1:] in self.commit_hashes:
            commit_hash = path[1:]

            ret = ['.', '..']

            for _, blob_or_tree, object_hash, filename in directory_of_commit(self.git_directory, commit_hash):
                ret += [filename]

            return ret

        else:

            ret = ['.', '..']

            path_parts = path[1:].split('/')
            commit_sha, path_parts = path_parts[0], path_parts[1:]

            object_hash = object_hash_of(
                self.git_directory,
                tree_of_commit(self.git_directory, commit_sha),
                path_parts
            )

            for _, blob_or_tree, object_hash, filename in directory_of_tree(self.git_directory, object_hash):
                ret += [filename]

            return ret

    def readlink(self, path):
        return self.data[path]

    def removexattr(self, path, name):
        print('removexattr??')
        raise FuseOSError(fuse.EROFS)

    def rename(self, old, new):
        raise FuseOSError(fuse.EROFS)

    def rmdir(self, path):
        raise FuseOSError(fuse.EROFS)

    def setxattr(self, path, name, value, options, position=0):
        raise FuseOSError(fuse.EROFS)

    def statfs(self, path):
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

    def symlink(self, target, source):
        raise FuseOSError(fuse.EROFS)

    def truncate(self, path, length, fh=None):
        raise FuseOSError(fuse.EROFS)

    def unlink(self, path):
        raise FuseOSError(fuse.EROFS)

    def utimens(self, path, times=None):
        raise FuseOSError(fuse.EROFS)

    def write(self, path, data, offset, fh):
        raise FuseOSError(fuse.EROFS)


if __name__ == '__main__':

    if len(argv) != 3:
        print('usage: %s <mountpoint> <path_to_git>' % argv[0])
        exit(1)

    logging.basicConfig(level=logging.DEBUG)
    fuse = FUSE(Git(argv[2]), argv[1], foreground=True)

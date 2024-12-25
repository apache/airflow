#!/bin/env python3
import fcntl
import json
import os

from git import Repo  # pip install gitpython

from airflow.models import Connection

GIT_PATH = "/tmp/git_repos/"


def sync_repo(conn_id: str, extra=None) -> str:
    """git repo 同步"""
    conn = Connection.get_connection_from_secrets(conn_id=conn_id)
    if extra is None:
        extra = json.loads(conn.extra)
    if conn.login:
        username = conn.login
    else:
        username = "oauth2"
    if conn.schema:
        schema = conn.schema
    else:
        schema = "https"
    token = conn.password
    git_repo = f"{schema}://{username}:{token}@{conn.host}"
    if extra.get("commit_id") is not None:
        commit = extra["commit_id"]
        reset_to = commit
        copy_to = ""
    elif extra.get("tag") is not None:
        commit = extra.get("tag")
        reset_to = commit
        copy_to = "&& git rev-parse HEAD | xargs -I{} rsync -a --delete `pwd`/ `pwd`/../{}"
    else:
        commit = extra.get("branch", "main")
        reset_to = "remotes/origin/" + commit
        copy_to = "&& git rev-parse HEAD | xargs -I{} rsync -a --delete `pwd`/ `pwd`/../{}"
    path_to_dir = f"{GIT_PATH}{conn_id}/{commit}"
    show_path = "&& git rev-parse HEAD | xargs -I{} echo `pwd`/../{}"
    if os.path.exists(path_to_dir):
        repo = Repo(path_to_dir)
    else:
        repo = Repo.init(path_to_dir)

    lock_file_path = path_to_dir + ".lock"
    if not os.path.exists(lock_file_path):
        open(lock_file_path, "w", encoding="utf-8").close()

    with open(path_to_dir + ".lock", "r", encoding="utf-8") as f:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        if "origin" not in repo.remotes:
            repo.create_remote("origin", git_repo)
        elif repo.remotes["origin"].url != git_repo:
            repo.remotes["origin"].set_url(git_repo)
        cmd = f"cd {path_to_dir} && git -c http.sslVerify=false fetch -f --depth 1 -q && git reset -q --hard {reset_to} {copy_to} {show_path}"
        return os.popen(cmd).read().strip()

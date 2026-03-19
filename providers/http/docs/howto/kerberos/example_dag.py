from __future__ import annotations

import logging
from typing import List
import os
import subprocess

from requests_kerberos import HTTPKerberosAuth, OPTIONAL

from airflow.security.kerberos import renew_from_kt

from airflow.providers.common.compat.sdk import DAG
from airflow.providers.http.operators.http import HttpOperator

PRINCIPAL_DOMAIN = "{login}@EXAMPLE.COM"

KEYTAB_PATH = "/tmp/{conn_id}.keytab"

KTUTIL_CMD = 'echo -e "add_entry -password -p {principal} -k 1 -e aes256-cts\\n{password}\\nwkt {keytab_path}" | ktutil \\\n&& echo -e "read_kt {keytab_path}\nlist" | ktutil && echo -e ""'

logger = logging.getLogger(__name__)

def _kerberos_auth_type(principal: str):
    """
    Workaround to pass in parameters to HttpHook session auth
    """
    def _inner_auth_type(*args, **kwargs):
        return HTTPKerberosAuth(principal=principal,mutual_authentication=OPTIONAL)
    
    return _inner_auth_type

def make_keytab(context):
    """
    Create a keytab from a username and password using ktutil
    
    :param context: Airflow task context
    """
    conn_id = context["task"].http_conn_id
    keytab = KEYTAB_PATH.format(conn_id=conn_id)

    if not os.path.exists(keytab):
        logger.info(f"Creating {keytab=}")
        conn = getattr(context["conn"],conn_id)
    
        principal = PRINCIPAL_DOMAIN.format(login=conn.login)

        ktutil_cmd = KTUTIL_CMD.format(principal=principal, password=conn.password, keytab_path=keytab)

        ktutil = subprocess.run(["bash", "-c", ktutil_cmd], stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE)
        return_code, stdout, stderr = ktutil.returncode, ktutil.stdout.decode(), ktutil.stderr.decode()
        if return_code != 0:
            raise RuntimeError(f"Failed to create keytab:\nstdout: {stdout}\nstderr: {stderr}")



def reinit_kerberos(context):
    """
    Run kinit to setup Kerberos authentication

    :param context: Airflow task context
    """

    task = context["task"]
    conn_id = task.http_conn_id
    keytab = KEYTAB_PATH.format(conn_id=conn_id)
    conn = getattr(context["conn"],conn_id)
    principal = PRINCIPAL_DOMAIN.format(login=conn.login)
    renew_from_kt(principal,keytab)
    task.auth_type = _kerberos_auth_type(principal)

def make_http_kerberos_task(task_id:str,http_conn_id:str = "http_default", endpoint:str = "/", create_keytab=True, **kwargs):
    """
    Create a HttpOperator using kerberos authentication.

    :param task_id: task id
    :param http_conn_id: Http connection to use
    :param endpoint: Operator endpoint
    :param create_keytab: Enables creating a keytab from user credentials.
    """
    callbacks = kwargs.pop("on_execute_callback",[])
    if not isinstance(callbacks,List):
        callbacks = [callbacks]

    if create_keytab:
        callbacks.append(make_keytab)
    callbacks.append(reinit_kerberos)

    return HttpOperator(
        task_id=task_id,
        http_conn_id=http_conn_id,
        endpoint=endpoint,
        on_execute_callback=callbacks,
        **kwargs,
        )

with DAG(dag_id="kerberos_http"):
    http_task = make_http_kerberos_task(task_id="kerberos_request")
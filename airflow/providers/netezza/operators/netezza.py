from typing import TYPE_CHECKING, Any, Callable, Iterable, Mapping, Optional, Sequence, Union

from airflow.models import BaseOperator
from airflow.providers.common.sql.hooks.sql import fetch_all_handler
from airflow.providers.netezza.hooks.netezza import NetezzaHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class NetezzaOperator(BaseOperator):
    """
    Executes sql code in Netezza database using nzpy driver.

    :param sql: the SQL code to be executed as a single string, or
        a list of str (sql statements), or a reference to a template file.
        Template references are recognized by str ending in '.sql'
    :param nz_conn_id: reference to a predefined database
    :param autocommit: if True, each command is automatically committed.
        (default value: False)
    :param parameters: (optional) the parameters to render the SQL query with.
    """

    template_fields: Sequence[str] = ('sql',)
    template_ext: Sequence[str] = ('.sql',)
    template_fields_renderers = {'sql': 'sql'}
    ui_color = '#ededed'

    def __init__(
        self,
        *,
        sql: Union[str, Iterable[str]],
        nz_conn_id: str = 'nz_default',
        autocommit: bool = False,
        parameters: Optional[Union[Iterable, Mapping]] = None,
        handler: Callable[[Any], Any] = fetch_all_handler,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.parameters = parameters
        self.sql = sql
        self.nz_conn_id = nz_conn_id
        self.autocommit = autocommit
        self.handler = handler
        self.hook = None

    def execute(self, context: 'Context'):
        self.log.info('Executing: %s', self.sql)
        hook = NetezzaHook(nz_conn_id=self.nz_conn_id)
        if self.do_xcom_push:
            return hook.run(self.sql, self.autocommit, parameters=self.parameters, handler=self.handler)
        else:
            return hook.run(self.sql, self.autocommit, parameters=self.parameters)

from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from typing import Optional, List, Callable, Dict, Union, Mapping, Iterable
from airflow.utils.operator_helpers import determine_kwargs


class Csv2DbOperator(MsSqlOperator):
    def __init__(
            self,
            *,
            mssql_conn_id: str = 'mssql_default',
            table: str,
            file_path: str,
            **kwargs
    ) -> None:
        self.sql = None
        kwargs['mssql_conn_id'] = mssql_conn_id
        super().__init__(**kwargs)
        self.table = table
        self.file_path = file_path

    def execute(self, context: Dict):
        return

from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.operators.python import PythonOperator
from typing import Optional, List, Callable, Dict
from airflow.utils.operator_helpers import determine_kwargs


class Redcap2DbOperator(PythonOperator):
    def __init__(
            self,
            *,
            conn_id: str,
            table: str,
            export_content: str = 'records',
            python_callable: Callable,
            op_args: Optional[List] = None,
            op_kwargs: Optional[Dict] = None,
            templates_dict: Optional[Dict] = None,
            templates_exts: Optional[List[str]] = None, **kwargs
    ) -> None:
        super().__init__(
            python_callable=python_callable,
            op_args=op_args,
            op_kwargs=op_kwargs,
            templates_dict=templates_dict,
            templates_exts=templates_exts,
            **kwargs
        )
        self.conn_id = conn_id
        self.table = table
        self.export_content = export_content

    def execute(self, context: dict) -> None:
        # First, do PythonOperator a few of the standard `execute` procedures
        context.update(self.op_kwargs)
        context['templates_dict'] = self.templates_dict
        self.op_kwargs = determine_kwargs(self.python_callable, self.op_args, context)
        return_value = self.execute_callable()

        # Second, apply the overriding
        # get data into list of tuples
        data = [tuple(entry.get(key, None) for key in entry.keys()) for entry in return_value]

        # get connection
        hook = MsSqlHook(mssql_conn_id=self.conn_id)
        hook.insert_rows(self.table, data, target_fields=return_value[0].keys())


class Db2RedcapOperator(PythonOperator):
    def __init__(
            self,
            *,
            conn_id: str,
            table: str,
            form: str,
            python_callable: Callable,
            op_args: Optional[List] = None,
            op_kwargs: Optional[Dict] = None,
            templates_dict: Optional[Dict] = None,
            templates_exts: Optional[List[str]] = None, **kwargs
    ) -> None:
        super().__init__(
            python_callable=python_callable,
            op_args=op_args,
            op_kwargs=op_kwargs,
            templates_dict=templates_dict,
            templates_exts=templates_exts,
            **kwargs
        )
        self.conn_id = conn_id
        self.table = table
        self.form = form

    def execute(self, context: Dict) -> None:
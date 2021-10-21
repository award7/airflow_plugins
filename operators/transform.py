from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from typing import Optional, List, Callable, Dict
from airflow.utils.operator_helpers import determine_kwargs


class TransformOperator(PythonOperator):
    def __init__(
            self,
            *,
            conn_id: str,
            table: str,
            sql: str,
            parameters: str,
            sql_kwargs: Optional[Dict] = None,
            output_file: str,
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
        self.sql = sql
        self.parameters = parameters
        self.sql_kwargs = sql_kwargs
        self.output_file = output_file

    def execute(self, context: Dict):
        # pull data from staging
        hook = MsSqlHook(mssql_conn_id=self.conn_id)
        df = hook.get_pandas_df(self.sql, self.parameters, **self.sql_kwargs)

        # append df to op_args list
        self.op_args.append(df)

        # apply transforms
        # do PythonOperator a few of the standard `execute` procedures
        context.update(self.op_kwargs)
        context['templates_dict'] = self.templates_dict
        self.op_kwargs = determine_kwargs(self.python_callable, self.op_args, context)
        return_value = self.execute_callable()

        # save output
        return_value.to_csv(self.output_file, index=False)

        # xcom push
        return self.output_file

from airflow.models.baseoperator import BaseOperator
from typing import Callable
import matlab.engine


class MatlabOperator(BaseOperator):

    """
    Execute MATLab via matlab.engine in python

    inspired by https://github.com/LREN-CHUV/airflow-imaging-plugins/blob/master/airflow_spm/operators/spm_operator.py
    and extended to be more general for matlab engine rather than just SPM
    """

    ui_color = '#FB4D27'

    def __init__(
            self,
            *,
            matlab_function: str,
            matlab_function_path: str,
            op_args: list = None,
            op_kwargs: list = None,
            nargout: int = 0,
            **kwargs):

        super().__init__(**kwargs)
        self.matlab_function = matlab_function
        self.matlab_function_path = matlab_function_path
        self.op_args = op_args
        self.op_kwargs = op_kwargs
        self.nargout = nargout
        self.engine = None

    def execute(self, context):
        self.engine = matlab.engine.start_matlab()

        if self.engine:
            # the matlab-python engine cannot unpack kwargs but can unpack positional args. So as a workaround, we'll
            # unpack them here and append to op_args
            for key, val in self.op_kwargs:
                self.op_args.append(key)
                self.op_args.append(val)

            result = getattr(self.engine, self.matlab_function)(*self.op_args, nargout=self.nargout)

            self.engine.exit()
            self.engine = None
        else:
            raise Exception

        # ti = context['ti']
        # for value, idx in enumerate(result):
        #     if len(result) < 2:
        #         idx = ''
        #     ti.xcom_push(key=f'return_value{idx}', value=value)

    def on_kill(self):
        if self.engine:
            self.engine.exit()
            self.engine = None

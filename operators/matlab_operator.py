from airflow.models.baseoperator import BaseOperator
import logging


try:
    import matlab.engine
except(IOError, RuntimeError, ImportError):
    logging.error('Matlab not available on this system')


class MatlabOperator(BaseOperator):

    """
    Execute MATLab via matlab.engine in python

    inspired by https://github.com/LREN-CHUV/airflow-imaging-plugins/blob/master/airflow_spm/operators/spm_operator.py
    and extended to be more general for matlab engine rather than just SPM
    """

    ui_color = '#FB4D27'

    def __init__(
            self,
            matlab_function,
            m_dir=None,
            op_args=None,
            op_kwargs=None,
            *args,
            **kwargs):

        super().__init__(**kwargs)
        self.matlab_function = matlab_function
        self.m_dir = m_dir
        self.op_args = op_args
        self.op_kwargs = op_kwargs
        self.engine = None

    def pre_execute(self, context):
        if matlab.engine:
            self.engine = matlab.engine.start_matlab()
        if self.engine:
            if self.m_dir is not None:
                self.engine.addpath(self.m_dir)
            logging.info("Matlab started...")
        else:
            msg = 'Matlab has not started on this node'
            logging.error(msg)
            raise

    def execute(self, context):
        if self.engine:
            result = getattr(self.engine, self.matlab_function)(*self.op_args,
                                                                nargout=self.op_kwargs['nargout'])
            print(result)
            return result
        else:
            msg = 'Matlab has not started on this node'
            logging.error(msg)
            raise

    def on_kill(self):
        if self.engine:
            self.engine.exit()
            self.engine = None

    def post_execute(self, context, **kwargs):
        if self.engine:
            self.engine.exit()
            self.engine = None

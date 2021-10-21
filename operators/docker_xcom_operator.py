from airflow.providers.docker.operators.docker import DockerOperator


class DockerXComOperator(DockerOperator):
    """
    Inherited DockerOperator class to get a valid xcom following container execution AND have the container
    be removed.
    To ensure the xcom is pushed, `auto_remove` must be `False` in the DockerOperator instantiation

    source: https://stackoverflow.com/questions/59184551/how-to-use-xcom-push-true-and-auto-remove-true-at-the-same-time-when-using-docke#answer-61511481
    """

    def post_execute(self, context, result=None):
        if self.cli is not None:
            self.log.info('Removing Docker container')
            self.cli.remove_container(self.container['Id'])
        super().post_execute(context, result)

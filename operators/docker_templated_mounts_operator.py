from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from typing import Any


"""
sometimes, the docker operator fails stating a 'permission denied', in which case the permissions for the .sock needs to
be adjusted via chmod 777 /var/run/docker.sock
https://stackoverflow.com/questions/56782039/permission-issue-on-running-docker-command-in-python-subprocess-through-apache-a
"""

class DockerTemplatedMountsOperator(DockerOperator):
    """
    Allows more fields to be templated for more dynamic operator
    """
    template_fields = ('command', 'environment', 'container_name', 'mounts')

    def __init__(
            self,
            **kwargs
    ) -> None:
        super().__init__(
            **kwargs
        )

    def pre_execute(self, context: Any) -> None:
        # make mounts
        _mounts_local = []
        for mount in self.mounts:
            target = mount['target']
            source = mount['source']
            mount_type = mount['type']

            _mounts_local.append(
                Mount(
                    target=target,
                    source=source,
                    type=mount_type,
                )
            )

        self.mounts = _mounts_local

from airflow.models import BaseOperator
import docker
from typing import Any


class DockerRemoveImage(BaseOperator):
    def __init__(self,
                 *,
                 image: str,
                 force: bool = False,
                 noprune: bool = False,
                 **kwargs):
        super().__init__(**kwargs)
        self.image = image
        self.force = force
        self.noprune = noprune

    def execute(self, context: Any) -> None:
        client = docker.from_env()
        try:
            client.images.remove(image=self.image, force=self.force, noprune=self.noprune)
        except docker.errors.ImageNotFound:
            return
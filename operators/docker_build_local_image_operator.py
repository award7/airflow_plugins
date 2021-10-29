from airflow.models import BaseOperator
import docker
from typing import Any


class DockerBuildLocalImageOperator(BaseOperator):
    template_fields = ["path"]

    def __init__(
            self,
            *,
            path: str = None,
            tag: str = None,
            fileobj: object = None,
            quiet: bool = False,
            nocache: bool = False,
            rm: bool = False,
            timeout: int = None,
            custom_context: bool = False,
            encoding: str = None,
            pull: bool = False,
            forcerm: bool = False,
            dockerfile: str = None,
            buildargs: dict = None,
            container_limits: dict = None,
            decode: bool = False,
            shmsize: int = None,
            labels: dict = None,
            cache_from: list = None,
            target: str = None,
            network_mode: str = None,
            squash: bool = False,
            extra_hosts: dict = None,
            platform: str = None,
            isolation: str = None,
            use_config_proxy: bool = True,
            gzip = False,
            **kwargs
    ):
        super().__init__(**kwargs)
        self.path = path
        self.tag = tag
        self.fileobj = fileobj
        self.quiet = quiet
        self.nocache = nocache
        self.rm = rm
        self.timeout = timeout
        self.custom_context = custom_context
        self.encoding = encoding
        self.pull = pull
        self.forcerm = forcerm
        self.dockerfile = dockerfile
        self.buildargs = buildargs
        self.container_limits = container_limits
        self.decode = decode
        self.shmsize = shmsize
        self.labels = labels
        self.cache_from = cache_from
        self.target = target
        self.network_mode = network_mode
        self.squash = squash
        self.extra_hosts = extra_hosts
        self.platform = platform
        self.isolation = isolation
        self.use_config_proxy = use_config_proxy
        self.gzip = gzip

    def execute(self, context: Any):
        client = docker.from_env()
        try:
            if self.tag:
                client.images.get(self.tag)
        except docker.errors.ImageNotFound:
            client.images.build(
                path=self.path,
                tag=self.tag,
                fileobj=self.fileobj,
                quiet=self.quiet,
                nocache=self.nocache,
                rm=self.rm,
                timeout=self.timeout,
                custom_context=self.custom_context,
                encoding=self.encoding,
                pull=self.pull,
                forcerm=self.forcerm,
                dockerfile=self.dockerfile,
                buildargs=self.buildargs,
                container_limits=self.container_limits,
                decode=self.decode,
                shmsize=self.shmsize,
                labels=self.labels,
                cache_from=self.cache_from,
                target=self.target,
                network_mode=self.network_mode,
                squash=self.squash,
                extra_hosts=self.extra_hosts,
                platform=self.platform,
                isolation=self.isolation,
                use_config_proxy=self.use_config_proxy,
                gzip=self.gzip
            )
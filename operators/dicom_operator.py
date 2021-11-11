from airflow.models.baseoperator import BaseOperator
from dicomsort.dicomsorter import DicomSorter
from typing import Any


class DicomSortOperator(BaseOperator):
    def __init__(
            self,
            *,
            source: str,
            target: str,
            filename: str or None,
            sort_order: list or None,
            anonymization: dict or None,
            keep_original=True,
            ignore: dict or None,
            **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.source = source
        self.target = target
        self.filename = filename
        self.sort_order = sort_order
        self.anonymization = anonymization
        self.keep_original = keep_original
        self.ignore = ignore

    def execute(self, context: Any) -> None:
        dcm = DicomSorter(
            source=self.source,
            target=self.target,
            filename=self.filename,
            sort_order=self.sort_order,
            anonymization=self.anonymization,
            keep_original=self.keep_original,
            ignore=self.ignore
        )
        dcm.sort()

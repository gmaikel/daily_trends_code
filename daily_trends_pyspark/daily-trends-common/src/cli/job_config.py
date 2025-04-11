from dataclasses import dataclass
from datetime import datetime

@dataclass
class JobConfig:
    source: str = None
    destination: str = None
    distributor: str = None
    dsp: str = None
    id_sub_platform: int = 0
    date: datetime = None
    report_id: str = None
    report_iteration: int = 1
    debug: bool = False
    local: bool = False
    local_catalog: str = None
    retry: bool = False
    retry_number: int = 0

    def __post_init__(self):
        if self.id_sub_platform is None:
            self.id_sub_platform = 0
        if self.report_iteration is None:
            self.report_iteration = 1
        if self.retry_number is None:
            self.retry_number = 0

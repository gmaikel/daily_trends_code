from dataclasses import dataclass
from datetime import datetime

@dataclass
class JobConfig:
    def __init__(self,
                 source: str=None,
                 destination: str=None,
                 distributor: str=None,
                 dsp: str=None,
                 id_sub_platform: int=0,
                 date: datetime=None,
                 report_id: str=None,
                 report_iteration: int=1,
                 debug: bool=False,
                 local: bool=False,
                 local_catalog: str=None,
                 retry: bool=False,
                 retry_number: int=0):
        self.source = source
        self.destination = destination
        self.distributor = distributor
        self.dsp = dsp
        self.id_sub_platform = id_sub_platform
        self.date = date
        self.report_id = report_id
        self.report_iteration = report_iteration
        self.debug = debug
        self.local = local
        self.local_catalog = local_catalog
        self.retry = retry
        self.retry_number = retry_number


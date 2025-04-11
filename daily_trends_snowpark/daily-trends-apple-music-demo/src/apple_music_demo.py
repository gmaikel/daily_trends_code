from datetime import date
from common.src.cli.job_config import JobConfig

STREAMS_PATTERN = "streams_*.csv"
DSP_ERRORS = "dsp_errors"

DATE_2023_04_24 = date(2023, 4, 24)

def streams_path(config: JobConfig) -> str:
    return f"{config.source}/{STREAMS_PATTERN}"

def dsp_errors_path(config: JobConfig) -> str:
    return f"{config.destination}/{DSP_ERRORS}"

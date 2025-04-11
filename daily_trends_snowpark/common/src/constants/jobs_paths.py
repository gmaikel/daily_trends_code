from job_config import JobConfig

class JobsPaths:
    DSP_ERRORS = "dsp_errors"
    STRUCTURING_PATH = "structuring.parquet"
    ENRICHING_PATH = "enriching.parquet"
    VALIDATING_PATH = "validating.parquet"

    TRACKS_WITH_REFERENCE = "tracks_with_reference.delta"
    PRODUCT_WITH_REFERENCE = "products_with_reference.delta"
    REJECTS_SUMMARY_PATH = "rejects_summary.json"
    STRUCTURING_REJECTS_SUMMARY_PATH = "rejects_structuring_summary.json"

    MISSING_URIS_PATH = "missing_tracks_uri_in_referential.parquet"
    TRACKS_WITH_REFERENCE_TUNECORE = "tracks_with_reference_new_tunecore_ref.delta"
    VALIDATING_PATH_TUNECORE = "validating_new_tunecore_ref.parquet"
    REJECTS_SUMMARY_PATH_TUNECORE = "rejects_summary_new_tunecore_ref.json"

    @staticmethod
    def dsp_errors_path(config: JobConfig) -> str:
        return f"{config.destination}/{JobsPaths.DSP_ERRORS}"

    @staticmethod
    def structuring_path(config: JobConfig) -> str:
        return f"{config.destination}/{JobsPaths.STRUCTURING_PATH}"

    @staticmethod
    def enriching_path(config: JobConfig) -> str:
        return f"{config.destination}/{JobsPaths.ENRICHING_PATH}"

    @staticmethod
    def validating_path(config: JobConfig) -> str:
        return f"{config.destination}/{JobsPaths.VALIDATING_PATH}"

    @staticmethod
    def rejects_summary_path(config: JobConfig) -> str:
        return f"{config.destination}/{JobsPaths.REJECTS_SUMMARY_PATH}"

    @staticmethod
    def structuring_rejects_summary_path(config: JobConfig) -> str:
        return f"{config.destination}/{JobsPaths.STRUCTURING_REJECTS_SUMMARY_PATH}"

    @staticmethod
    def tracks_with_referential(config: JobConfig) -> str:
        return f"{config.destination}/{JobsPaths.TRACKS_WITH_REFERENCE}"

    @staticmethod
    def product_with_referential(config: JobConfig) -> str:
        return f"{config.destination}/{JobsPaths.PRODUCT_WITH_REFERENCE}"

    @staticmethod
    def tracks_with_referential_tunecore(config: JobConfig) -> str:
        return f"{config.destination}/{JobsPaths.TRACKS_WITH_REFERENCE_TUNECORE}"

    @staticmethod
    def validating_path_tunecore(config: JobConfig) -> str:
        return f"{config.destination}/{JobsPaths.VALIDATING_PATH_TUNECORE}"

    @staticmethod
    def missing_uris_path_tunecore(config: JobConfig) -> str:
        return f"{config.destination}/{JobsPaths.MISSING_URIS_PATH}"

    @staticmethod
    def rejects_summary_path_tunecore(config: JobConfig) -> str:
        return f"{config.destination}/{JobsPaths.REJECTS_SUMMARY_PATH_TUNECORE}"

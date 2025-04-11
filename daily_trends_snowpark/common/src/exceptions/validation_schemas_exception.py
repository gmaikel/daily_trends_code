from pyspark.sql.types import StructType


class ValidationSchemaException(Exception):
    """
    Custom exception for validation schema errors.
    """
    def __init__(self, message: str):
        super().__init__(message)


class ValidationSchemaExceptionUtil:
    @staticmethod
    def buildMessage(message: str, actual: StructType, expected: StructType) -> str:
        """
        Build the error message string for validation schema exceptions.
        """
        return f"""{message}
actual : {actual}
expected : {expected}
"""

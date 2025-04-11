import json
from pyspark.sql import SparkSession
from pyspark import SparkFiles
from pathlib import Path
import os

class JsonFsUtils:

    @staticmethod
    def overwrite(path: str, data: dict):
        """
        Overwrites the file at the specified path with the provided dictionary.

        :param path: The path where the file should be written.
        :param data: The dictionary to write.
        """
        # Convert the dictionary to JSON
        json_data = json.dumps(data, indent=4, sort_keys=True)

        # Create the path object
        path_obj = Path(path)

        # Check if the file exists and delete it
        if path_obj.exists():
            os.remove(path_obj)

        # Write the data to the file
        with open(path_obj, 'w') as out_file:
            out_file.write(json_data)

    @staticmethod
    def read(path: str) -> dict:
        """
        Reads the JSON file from the given path and returns its contents as a dictionary.

        :param path: The path to the file to read.
        :return: The data as a dictionary.
        """
        path_obj = Path(path)

        if path_obj.exists():
            with open(path_obj, 'r') as in_file:
                return json.load(in_file)
        else:
            return {}
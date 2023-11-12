"""
Module for parsing files
"""
import csv
import json
import pandas as pd

import yaml


class FileParser:
    """
    Class for parsing various types of files into lists of dicts
    """

    file_extensions = ["json", "yaml", "yml", "csv", "xlsx"]

    def __init__(self, file_path=None, config=None):

        self.file_path = file_path
        self.data_list: list = []
        self.file_type = self.find_file_type(file_path)

        if config is None:
            config = {}

        if isinstance(config, str):
            self.config = self.read_yaml(config)
        elif isinstance(config, dict):
            self.config = config

    def find_file_type(self, file_path: str):
        """
        Discover the extension of the file path provided
        :param file_path: the path of the file
        """
        file_type = None

        path_parts = file_path.split(".")
        for word in path_parts:

            if word in FileParser.file_extensions:
                file_type = word

        return file_type

    @staticmethod
    def read_yaml(file_path: str, encoding: str = "utf-8") -> dict | list:
        """
        Read a yaml file into memory
        :param file_path: the path to the file to read
        :param encoding: the encoding to use
        """
        data = {}

        try:
            with open(file_path, "r", encoding=encoding) as file:
                data = yaml.safe_load(file)
        except FileNotFoundError as exc:
            raise FileNotFoundError(f"No file found at {file_path}") from exc

        return data

    @staticmethod
    def read_json(file_path: str, encoding: str = "utf-8"):
        """
        Read a json file into memory
        :param file_path: the path to the file to read
        :param encoding: the encoding to use
        """
        data = None

        try:
            with open(file_path, "r", encoding=encoding) as file:
                data = json.loads(file.read())
        except FileNotFoundError as exc:
            raise FileNotFoundError(f"No file found at {file_path}") from exc

        return data

    @staticmethod
    def read_csv(file_path: str, encoding: str = "utf-8-sig"):
        """
        Read a csv file into memory
        :param file_path: the path to the file to read
        :param encoding: the encoding to use
        """
        try:
            data = pd.read_csv(file_path)

        except FileNotFoundError as exc:
            raise FileNotFoundError(f"No file found at {file_path}") from exc

        return data

    @staticmethod
    def write_csv(output_file_path: str, header: list, encoding: str = "utf-8-sig"):
        """
        Write a csv file into memory
        :param file_path: the path to the file to read
        :param encoding: the encoding to use
        """
        csvfile = open(output_file_path, 'w', newline='')
        fieldnames = header
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        return csvfile, writer

    @staticmethod
    def df_to_csv(output_file_path: str, df: None, index: bool = None,  encoding: str = "utf-8"):
        """
        convert dataframe to csv and Write a csv file into memory
        :param output_file_path: the path to the file to save
        :param df: pandas dataframe
        :param index: index option
        :param encoding: the encoding to use
        """
        df.to_csv(output_file_path, index=index, encoding=encoding)

    @staticmethod
    def filter_nan_values(df: None):
        """
        filter nan values from dataframe
        :param df: pandas dataframe
        return dataframe
        """
        columns_to_check = df.columns
        mask = df[columns_to_check].apply(
            lambda col: col.notna().all(), axis=1)
        filtered_dataframe = df[mask]
        return filtered_dataframe

    def read_file(self):
        """
        Read a file using the internal parameters given to the file parser
        """
        if self.config is None and self.file_path is None:
            raise ValueError(
                "Config and/or file path must be given to the parser's constructor"
            )
        data = None
        if self.file_type in ["yaml", "yml"]:
            data = FileParser.read_yaml(self.file_path)
        elif self.file_type in ["json"]:
            data = FileParser.read_json(self.file_path)
        elif self.file_type == "csv":
            data = FileParser.read_csv(self.file_path)

        if isinstance(data, dict):
            self.data_list.append(data)
        if isinstance(data, list):
            self.data_list += data

        return data

    # def item_generator(self):
    #     """
    #     Offer up items in the file one at a time
    #     """
    #     self.read_file()

    #     for item in self.data_list:
    #         yield item

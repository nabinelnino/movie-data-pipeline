"""
Module for interfacing with databases and provides tools to bulk load
"""
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


class DatabaseHandle:
    """
    Handle class for interfacing with databases
    """

    def __init__(self, connection_string: str = None):
        self.connection_string = connection_string
        self.engine = None
        self.session = None

    async def connect(self, connection_string: str = None):
        """
        Connect to a database prioritizing the connection string provided to the function
        otherwise using the connection string given to the constructor
        :throws ValueError: if no connection string was found on the class or given to the function
        :param connection_string: the connection string used to connect to a database
        """
        con_str = (
            connection_string
            if connection_string is not None
            else self.connection_string
        )

        if not con_str:
            raise ValueError(
                "Connection string must be provided to the constructor or connect. None was found"
            )

        self.engine = create_engine(con_str)
        session = sessionmaker(bind=self.engine)
        self.session = session()

    async def insert_data(self, data: dict | list, model, unique_key_list: list = None):
        """
        Insert data into the connected database given a
        model and a list of dicts or a dict of data
        :param data: the data to give (dict or list)
        :param model: the sqla model to use for inserting the data
        :throws ValueError: if the given data is not a dict or a list
        """
        model.base.metadata.create_all(bind=self.engine)
        data_list = []
        if isinstance(data, dict):
            data_list.append(data)
        elif isinstance(data, list):
            data_list = data
        else:
            raise ValueError("data must be a list of dicts or a dict")

        for item in data_list:
            database_item = model(**item)

            if unique_key_list:
                if not await self._item_exists(item, model, unique_key_list):
                    self.session.add(database_item)
            else:
                self.session.add(database_item)

    async def commit(self):
        """
        Commits any staged database changes
        """
        if self.session:
            self.session.commit()

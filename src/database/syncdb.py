"""
Module functionality for communicating data with the database
"""
import os

from time import perf_counter
from sqlalchemy import create_engine, Column, Integer, String, Sequence
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


from .models.movie_model import MovieModel


class Synchronizer:
    """
    Class to connect to a data source and load into a connected database
    """

    def __init__(
        self,
        connection_str: str,
        data_model: None,

    ):
        self.connection_str = connection_str
        self.data_model = data_model
        # self.connect(self.connection_str)
        self.engine = None
        self.session = None

    def connect(self, connection_string: str = None):
        """
        Connect to a database prioritizing the connection string provided to the function
        otherwise using the connection string given to the constructor
        :throws ValueError: if no connection string was found on the class or given to the function
        :param connection_string: the connection string used to connect to a database
        """
        con_str = (
            connection_string
            if connection_string is not None
            else self.connection_str
        )

        if not con_str:
            raise ValueError(
                "Connection string must be provided to the constructor or connect. None was found"
            )

        self.engine = create_engine(con_str)
        session = sessionmaker(bind=self.engine)

        self.session = session()

    async def insert_data(self, data):
        """
        Insert data into connected database after running the data through
        the applied set of transforms
        :param data: an iterable data source containing dicts that match the data model
        """

        print("- Starting data ingestion")

        self.connect()

        self.data_model.base.metadata.create_all(bind=self.engine)

        data_list = []
        if isinstance(data, dict):
            data_list.append(data)
        elif isinstance(data, list):
            data_list = data
        else:
            raise ValueError("data must be a list of dicts or a dict")

        for item in data_list:

            database_item = MovieModel(**item)

            self.session.add(database_item)

        print("- Committing data to database")

        await self.commit()
        self.session.close()

    async def commit(self):
        """
        Commits any staged database changes
        """
        if self.session:
            self.session.commit()


async def create_connection(data: None, data_model: None):
    """
    Create a connection
    :param data: an iterable data source containing dicts that match the data model
    :data_model: model class that match the database columns
    """
    start_time = perf_counter()

    db_port = os.getenv("DATABASE_PORT", 5432)
    db_host = os.getenv("DATABASE_HOST", "localhost")
    db_user = os.getenv("DATABASE_USERNAME", "postgres")
    db_password = os.getenv("DATABASE_PASSWORD", "admin")
    db_database = os.getenv("DATABASE_DB", "noname")

    if not (db_host and db_port and db_user and db_password and db_database):
        raise ValueError(
            "Database Connection ENV vars need to be set, Got: "
            + f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_database}"
        )

    database_connection = (
        f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_database}"
    )

    sync = Synchronizer(database_connection, data_model)

    await sync.insert_data(data)
    print(f"- Execution time: {perf_counter() - start_time}")
    return 0


async def sync_main(data, data_model):
    """
    Run the main functions
    """
    await create_connection(data, data_model)

"""
Module for base models
"""
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()


class BaseModel(Base):
    """
    Base model for all database models
    """

    __abstract__ = True
    base = Base

    def __init__(self, **kwargs) -> None:
        for k, value in kwargs.items():
            if hasattr(self.__class__, k):
                if isinstance(value, list):
                    value = str(value)
                elif isinstance(value, dict):
                    value = str(value)
                setattr(self, k, value)
            else:
                print(
                    "Warning! Model given column that "
                    + f"does not exist on table - name: {k} value: {value}"
                )

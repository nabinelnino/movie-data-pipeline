"""
Module for movie_data table definitions
"""
from sqlalchemy import Column
from sqlalchemy import Integer, String, Float, DateTime
from .base_model import BaseModel
import uuid


class MovieModel(BaseModel):
    """
    model for movie data tables
    """

    __tablename__ = "movie_data"

    id = Column("id", Integer, primary_key=True, default=str(uuid.uuid4()))
    movie_id = Column("movie_id", String)
    year = Column("year", Integer)
    genre = Column("genre", String)
    frequency = Column("frequency", Integer)
    movie_name = Column("movie_name", String)
    created_at = Column("created_at", DateTime(timezone=True), index=True)
    link = Column("link", String, index=True)

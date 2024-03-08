"""
database preparation function
"""
# -*- coding: utf-8 -*-
def prepare(engine):
    """ setup function"""
    # ``Base`` is a declarative_base object used for ORM classes.
    from .orm import Base

    # Need to import models so Base.metadata is aware of their existence
    from .orm import DownloadRegistry

    # Binds the SQLAlchemy engine to the models' metadata so they can be accessed
    Base.metadata.create_all(engine)

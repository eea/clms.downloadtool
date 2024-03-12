"""
database preparation function
"""
# -*- coding: utf-8 -*-


def prepare(engine):
    """ setup function"""
    # ``Base`` is a declarative_base object used for ORM classes.
    # Need to import models so Base.metadata is aware of their existence
    # pylint disable=import-outside-toplevel
    # pylint disable=unused-import
    from .orm import Base, DownloadRegistry  # noqa

    # Binds the SQLAlchemy engine to the models' metadata so they can be
    # accessed
    Base.metadata.create_all(engine)

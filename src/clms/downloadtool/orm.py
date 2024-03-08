"""
ORM for download registry
"""
# -*- coding: utf-8 -*-
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from z3c.saconfig import named_scoped_session


class Base(DeclarativeBase):
    """ declarative base"""


class DownloadRegistry(Base):
    """ base class """
    __tablename__ = 'downloadregistry'

    id: Mapped[str] = mapped_column(primary_key=True)
    content: Mapped[str] = mapped_column()


Session = named_scoped_session("download_registry")

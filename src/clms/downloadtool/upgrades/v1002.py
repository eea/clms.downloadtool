""" upgrade step implementation """
# -*- coding: utf-8 -*-

from logging import getLogger

logger = getLogger(__name__)


def upgrade(setup_tool=None):
    """upgrade function"""
    logger.info("Running upgrade (Python): New field in controlpanel")

# -*- coding: utf-8 -*-
"""Installer for the clms.downloadtool package."""

from setuptools import find_packages
from setuptools import setup
readme_data = ""
contributors_data = ""
changes_data = ""

NAME = "clms.downloadtool"
PATH = NAME.split('.') + ['version.txt']
VERSION = open(join(*PATH)).read().strip()
setup(
    name=NAME,
    version=VERSION,
    description="An add-on for Plone",
    long_description_content_type="text/x-rst",
    long_description=(
          open("README.rst").read() + "\n" +
          open(join("docs", "HISTORY.txt")).read()
      ),
    # Get more from https://pypi.org/classifiers/
    classifiers=[
        "Environment :: Web Environment",
        "Framework :: Plone",
        "Framework :: Plone :: Addon",
        "Framework :: Plone :: 5.2",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3.7",
        "Operating System :: OS Independent",
        "License :: OSI Approved :: GNU General Public License v2 (GPLv2)",
    ],
    keywords="Python Plone CMS",
    author="Mikel Larreategi",
    author_email="mlarreategi@codesyntax.com",
    url="https://github.com/collective/clms.downloadtool",
    project_urls={
        "PyPI": "https://pypi.python.org/pypi/clms.downloadtool",
        "Source": "https://github.com/collective/clms.downloadtool",
        "Tracker": "https://github.com/collective/clms.downloadtool/issues",
    },
    license="GPL version 2",
    packages=find_packages("src", exclude=["ez_setup"]),
    namespace_packages=["clms"],
    package_dir={"": "src"},
    include_package_data=True,
    zip_safe=False,
    python_requires="==2.7, >=3.6",
    install_requires=[
        "setuptools",
        # -*- Extra requirements: -*-
        "z3c.jbot",
        "plone.api>=1.8.4",
        "plone.restapi",
        "plone.app.dexterity",
    ],
    extras_require={
        "test": [
            "plone.app.testing",
            # Plone KGS does not use this version, because it would break
            # Remove if your package shall be part of coredev.
            # plone_coredev tests as of 2016-04-01.
            "plone.testing>=5.0.0",
            "plone.app.contenttypes",
            "plone.app.robotframework[debug]",
        ],
    },
    entry_points="""
    [z3c.autoinclude.plugin]
    target = plone
    [console_scripts]
    update_locale = clms.downloadtool.locales.update:update_locale
    """,
)

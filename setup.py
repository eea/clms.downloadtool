# -*- coding: utf-8 -*-
"""Installer for the clms.downloadtool package."""
from os.path import join
from setuptools import find_packages
from setuptools import setup

readme = ""
history = ""
version = ""

NAME = "clms.downloadtool"
PATH = ["src"] + NAME.split(".") + ["version.txt"]

with open("README.rst") as readme_file:
    readme = readme_file.read()
with open(join("docs", "HISTORY.txt")) as history_file:
    history = history_file.read()
with open(join(*PATH)) as version_file:
    version = version_file.read().strip()

setup(
    name=NAME,
    version=version,
    description="An add-on for Plone",
    long_description_content_type="text/x-rst",
    long_description=(readme + "\n" + history),
    # Get more from https://pypi.org/classifiers/
    classifiers=[
        "Environment :: Web Environment",
        "Framework :: Plone",
        "Framework :: Plone :: Addon",
        "Framework :: Plone :: 6.0",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.11",
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
    python_requires=">=3.7",
    install_requires=[
        "setuptools",
        # -*- Extra requirements: -*-
        "plone.api",
        "plone.restapi",
        "plone.app.dexterity",
        "requests",
        "clms.types",
        "clms.statstool",
        "eea.cache",
        "PyProj",
    ],
    extras_require={
        "test": [
            "plone.app.testing",
            # Plone KGS does not use this version, because it would break
            # Remove if your package shall be part of coredev.
            # plone_coredev tests as of 2016-04-01.
            "plone.testing>=5.0.0",
            "plone.app.contenttypes",
            "plone.app.robotframework",
            "plone.restapi[test]",
        ],
    },
    entry_points="""
    [z3c.autoinclude.plugin]
    target = plone
    [console_scripts]
    update_locale = clms.downloadtool.locales.update:update_locale
    """,
)

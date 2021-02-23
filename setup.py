#!/usr/bin/env python
import os
import re

import setuptools

PACKAGE_NAME = "nba_crawler"
DESCRIPTION = "Utilities for crawling NBA Stats data"


def get_version(package_name):
    with open(os.path.join(package_name, "version.py")) as f:
        match = re.match('__version__ = "(.+)"', f.readline())
        return match.groups()[0]


setuptools.setup(
    name=PACKAGE_NAME,
    version=setuptools.sic(get_version(PACKAGE_NAME)),
    package_data={PACKAGE_NAME: ["py.typed"]},
    packages=setuptools.find_packages(exclude=["*.tests", "*.tests.*", "tests.*", "tests"]),
    description=DESCRIPTION,
    entry_points={"console_scripts": ["nba_crawler = nba_crawler.__main__:_nba_crawler"]},
    zip_safe=False,
)

#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-bitbucket",
    version="0.1.0",
    description="Singer.io tap for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_bitbucket"],
    install_requires=[
        # NB: Pin these to a more specific version for tap reliability
        "singer-python",
        "requests",
        "strict-rfc3339"
    ],
    entry_points="""
    [console_scripts]
    tap-bitbucket=tap_bitbucket:main
    """,
    packages=["tap_bitbucket"],
    package_data = {
        "schemas": ["tap_bitbucket/schemas/*.json"]
    },
    include_package_data=True,
)

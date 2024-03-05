"""
Setup script for dlt_sample.

This script packages and distributes the associated wheel file(s).
Source code is in ./src/. Run 'python setup.py sdist bdist_wheel' to build.
"""
from setuptools import setup, find_packages

import sys
sys.path.append('./src')

import dlt_sample

setup(
    name="dlt_sample",
    version=dlt_sample.__version__,
    url="https://databricks.com",
    author="emanuele.rinaldi@databricks.com",
    description="my test wheel",
    packages=find_packages(where='./src'),
    package_dir={'': 'src'},
    entry_points={"entry_points": "main=dlt_sample.main:main"},
    install_requires=["setuptools"],
)

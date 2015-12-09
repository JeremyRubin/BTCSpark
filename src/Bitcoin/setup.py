#!/usr/bin/env python
from setuptools import setup

setup(
    name='BTCLib',
    version='0.1',
    description='Bitcoin Python Spark code',
    author='Jeremy Rubin',
    dependency_links=["https://github.com/boto/boto/archive/2.38.0.tar.gz"],
    author_email='jr@mit.edu',
    packages=['BTCLib'])

#!/usr/bin/env python
from setuptools import setup
from Cython.Build import cythonize

setup(
    name='BTCLib',
    version='0.1',
    description='Bitcoin Python Spark code',
    author='Jeremy Rubin',
    dependency_links=["https://github.com/boto/boto/archive/2.38.0.tar.gz"],
    author_email='jr@mit.edu',
    packages=['BTCLib'],
    ext_modules = cythonize("BTCLib/native_lazy_blockchain.pyx"))

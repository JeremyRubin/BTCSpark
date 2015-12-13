"""
    Copyright 2015 Jeremy Rubin

    This program is free software: you can redistribute it and/or modify it
    under the terms of the Affero GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or (at your
    option) any later version.

    This program is distributed in the hope that it will be useful, but WITHOUT
    ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
    FITNESS FOR A PARTICULAR PURPOSE.  See the Affero GNU General Public
    License for more details.

    You should have received a copy of the Affero GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""
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

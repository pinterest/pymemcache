#!/usr/bin/env python

import os
import re

from setuptools import setup, find_packages


def read(path):
    return open(os.path.join(os.path.dirname(__file__), path)).read()


def read_version(path):
    match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]", read(path), re.M)
    if match:
        return match.group(1)
    raise RuntimeError("Unable to find __version__ in %s." % path)


readme = read('README.rst')
changelog = read('ChangeLog.rst')
version = read_version('pymemcache/__init__.py')

setup(
    name='pymemcache',
    version=version,
    author='Charles Gordon',
    author_email='charles@pinterest.com',
    packages=find_packages(),
    install_requires=['six'],
    description='A comprehensive, fast, pure Python memcached client',
    long_description=readme + '\n' + changelog,
    license='Apache License 2.0',
    url='https://github.com/Pinterest/pymemcache',
    classifiers=[
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: Implementation :: PyPy',
        'License :: OSI Approved :: Apache Software License',
        'Topic :: Database',
    ],
)

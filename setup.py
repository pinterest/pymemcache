#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name = 'pymemcache',
    version = '0.1',
    packages = find_packages(),
    setup_requires = ['nose>=1.0'],
)


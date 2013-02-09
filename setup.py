#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name = 'pymemcache',
    version = '0.9',
    author = 'Charles Gordon',
    author_email = 'charles@pinterest.com',
    packages = find_packages(),
    setup_requires = ['nose>=1.0'],
    description = 'A comprehensive, fast, pure Python memcached client',
    license = 'Apache License 2.0',
)


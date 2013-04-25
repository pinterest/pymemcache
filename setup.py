#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name = 'pymemcache',
    version = '1.0.1',
    author = 'Charles Gordon',
    author_email = 'charles@pinterest.com',
    packages = find_packages(),
    setup_requires = ['nose>=1.0'],
    description = 'A comprehensive, fast, pure Python memcached client',
    long_description = open('README.md').read(),
    license = 'Apache License 2.0',
    url = 'https://github.com/Pinterest/pymemcache',
)


#!/usr/bin/env python

from setuptools import setup, find_packages

from pymemcache import __version__

setup(
    name = 'pymemcache',
    version = __version__,
    author = 'Charles Gordon',
    author_email = 'charles@pinterest.com',
    packages = find_packages(),
    tests_require = ['nose>=1.0'],
    install_requires = ['six'],
    description = 'A comprehensive, fast, pure Python memcached client',
    long_description = open('README.md').read(),
    license = 'Apache License 2.0',
    url = 'https://github.com/Pinterest/pymemcache',
    classifiers = [
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.3',
        'License :: OSI Approved :: Apache Software License',
        'Topic :: Database',
    ],
)


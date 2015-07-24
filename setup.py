#!/usr/bin/env python
import os

from setuptools import setup, find_packages
from pymemcache import __version__


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

readme = read('README.rst')
changelog = read('ChangeLog.rst')


setup(
    name='pymemcache',
    version=__version__,
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
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: Implementation :: PyPy',
        'License :: OSI Approved :: Apache Software License',
        'Topic :: Database',
    ],
)

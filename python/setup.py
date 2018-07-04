#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
#  Copyright 2016-2018 Nathan Matare 
#  
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# "Author: Nathan Matare <nathan.matare@chicagobooth.edu>"

import os 
import io
from setuptools import setup, find_packages

# Package metadata.
name = 'putils'
description = 'Commonly used utility functions'
version = '0.0.9'
# Should be one of:
# Development Status :: 1 - Planning
# Development Status :: 2 - Pre-Alpha
# Development Status :: 3 - Alpha
# Development Status :: 4 - Beta
# Development Status :: 5 - Production/Stable
# Development Status :: 6 - Mature
# Development Status :: 7 - Inactive
release_status = 'Development Status :: 2 - Pre-Alpha'
dependencies = [
    'google-cloud-bigquery>=0.28.0',
    'google-cloud-storage>=1.6.0'
]

keywords='scaffolding python3 python-3.6 python R big-query google'
extras = None

# Boiler Plate
package_root = os.path.abspath(os.path.dirname(__file__))

readme_filename = os.path.join(package_root, 'README.md')
with io.open(readme_filename, encoding='utf-8') as readme_file:
    readme = readme_file.read()

setup(
    name=name,
    version=version,
    description=description,
    long_description=readme,
    author='Nathan Matare',
    author_email='nmatare@chicagobooth.edu',
    license='Apache 2.0',
    url='https://github.com/nmatare/utils',
    keywords=keywords,
    classifiers=[
        release_status,
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3.6',
        'Operating System :: OS Independent',
        'Topic :: Utilities',
    ],
    packages=find_packages(),
    install_requires=dependencies,
    extras_require=extras,
    include_package_data=True,
    zip_safe=False,
)


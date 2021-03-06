#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import codecs

from setuptools import setup
from setuptools import find_packages

here = os.path.abspath(os.path.dirname(__file__))
 
with codecs.open(os.path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = '\n' + f.read()

about = {}

with open(os.path.join(here, "bag3d", "__version__.py")) as f:
    exec (f.read(), about)

setup(name='bag3d',
    version=about['__version__'],
    description='A process for generating a 3D BAG',
    long_description=long_description,
    url='https://github.com/tudelft3d/bag3d',
    author='Balázs Dukai',
    author_email='b.dukai@tudelft.nl',
    license='GPLv3',
    packages=find_packages(),
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Science/Research',
        'Topic :: Scientific/Engineering :: GIS',
         'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Programming Language :: Python :: 3.6',
        'Operating System :: POSIX :: Linux'
    ],
    python_requires='>=3',
    keywords='GIS 3DGIS CityGML LiDAR',
    entry_points={
        'console_scripts': ['bag3d = bag3d.__main__:main']
    },
    include_package_data=True,
    zip_safe=False
    )

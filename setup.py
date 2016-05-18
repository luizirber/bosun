# -*- coding: utf-8 -*-
import sys
import os
import subprocess

from setuptools import setup, find_packages

PUBLISH_CMD = "python setup.py register sdist upload"
TEST_PUBLISH_CMD = 'python setup.py register -r test sdist upload -r test'
TEST_CMD = 'nosetests'

if 'publish' in sys.argv:
    status = subprocess.call(PUBLISH_CMD, shell=True)
    sys.exit(status)

if 'publish_test' in sys.argv:
    status = subprocess.call(TEST_PUBLISH_CMD, shell=True)
    sys.exit()

if 'run_tests' in sys.argv:
    try:
        __import__('nose')
    except ImportError:
        print('nose required. Run `pip install nose`.')
        sys.exit(1)
    status = subprocess.call(TEST_CMD, shell=True)
    sys.exit(status)

long_desc = ''' '''
requires = [
  'Fabric>=1.6.0',
  'PyYAML',
  'mom-utils',
  'python-dateutil',
  'rec_env'
]

setup(
    name='bosun',
    version='1.0.3',
    url='https://github.com/luizirber/bosun',
    download_url='https://github.com/luizirber/bosun',
    license='PSF',
    author=['Luiz Irber', 'Guilherme Castelao'],
    author_email=['luiz.irber@gmail.com', 'castelao@gmail.com'],
    description='Bosun is a runtime environment for BESM and associated models.',
    long_description=long_desc,
    zip_safe=False,
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: Python Software Foundation License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Topic :: Scientific/Engineering',
    ],
    platforms='any',
    scripts=["bin/bosun"],
    packages=find_packages(),
    include_package_data=True,
    install_requires=requires,
    tests_require=['nose'],
)

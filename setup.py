#!python
import sys
from setuptools import setup

extras = {}
if sys.version_info[0] >= 3:
    extras['use_2to3'] = True
    extras['use_2to3_exclude_fixers'] = ['lib2to3.fixes.fix_next',]

setup(name='pvc',
      version='0.0.1',
      description='Parameter Version Control',
      author='Chang She',
      install_requires=['pandas >= 0.8'],
      author_email='chang@lambdafoundry.com',
      url='http://www.github.com/changhiskhan/pvc/',
      platforms='any',
      test_suite='nose.collector',
      packages=['pvc'],
      zip_safe=False,
      **extras)

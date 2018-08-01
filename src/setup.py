"""
Setup configuration for the gsdm application

Written by DEIMOS Space S.L. (dibb)

module gsdm
"""
from setuptools import setup, find_packages

setup(name="gsdm",
      version="0.1.0",
      description="Ground Segment Data Management",
      url="https://bitbucket.org/dbrosnan/gsdm/",
      author="Daniel Brosnan",
      author_email="daniel.brosnan@deimos-space.com",
      packages=["gsdm.datamodel","gsdm.engine"],
      python_requires='>=3',
      install_requires=[
          "sqlalchemy",
          "psycopg2-binary",
          "geoalchemy2",
          "python-dateutil",
          "openpyxl",
          "lxml",
          "shapely",
          "matplotlib",
          "oslo.concurrency",
          "Pillow",
          "xmlschema",
          "jsonschema"
      ],
      tests_require=[
          "nose",
          "before_after",
          "coverage",
          "termcolor"
      ],
      test_suite='nose.collector')

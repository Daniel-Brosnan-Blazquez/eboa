"""
Setup configuration for the eboa application

Written by DEIMOS Space S.L. (dibb)

module eboa
"""
from setuptools import setup, find_packages

setup(name="eboa",
      version="0.1.0",
      description="Engine for Business Operation Analysis",
      url="https://bitbucket.org/dbrosnan/eboa/",
      author="Daniel Brosnan",
      author_email="daniel.brosnan@deimos-space.com",
      packages=["eboa","eboa.datamodel","eboa.engine", "ingestions.s2.functions"],
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

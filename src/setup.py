"""
Setup configuration for the eboa application

Written by DEIMOS Space S.L. (dibb)

module eboa
"""
from setuptools import setup, find_packages

setup(name="eboa",
      version="1.0.5",
      description="Engine for Business Operation Analysis",
      url="https://bitbucket.org/dbrosnan/eboa/",
      author="Daniel Brosnan",
      author_email="daniel.brosnan@deimos-space.com",
      packages=find_packages(),
      python_requires='>=3',
      install_requires=[
          "sqlalchemy==1.3.22",
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
          "jsonschema",
          "psutil",
          "inotify",
          "lockfile",
          "python-daemon",
          "scipy",
          "astropy",
          "pyquaternion",
          "sgp4"
      ],
      extras_require={
          "tests" :[
              "nose",
              "before_after",
              "coverage",
              "termcolor",
              "pytest-cov",
              "Sphinx"
          ]
      },
      test_suite='nose.collector')

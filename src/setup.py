"""
Setup configuration for the eboa application

Written by Daniel Brosnan Blázquez

module eboa
"""
from setuptools import setup, find_packages

setup(name="eboa",
      version="1.0.7",
      description="Engine for Business Operation Analysis",
      url="https://bitbucket.org/dbrosnan/eboa/",
      author="Daniel Brosnan",
      author_email="daniel.brosnan@deimos-space.com",
      packages=find_packages(),
      python_requires='>=3',
      install_requires=[
          "sqlalchemy==1.3.22",
          "psycopg2-binary==2.9.11",
          "geoalchemy2==0.11.1",
          "python-dateutil==2.9.0.post0",
          "openpyxl==3.1.5",
          "lxml==6.0.2",
          "shapely==2.0.7",
          "matplotlib==3.9.4",
          "oslo.concurrency==7.2.0",
          "Pillow==11.3.0",
          "xmlschema==4.2.0",
          "jsonschema==4.25.1",
          "psutil==7.2.2",
          "inotify==0.2.12",
          "lockfile==0.12.2",
          "python-daemon==3.1.2",
          "scipy==1.13.1",
          "astropy==6.0.1",
          "pyquaternion==0.9.9",
          "sgp4==2.25"
      ],
      extras_require={
          "tests" :[
              "nose==1.3.7",
              "before_after==1.0.1",
              "coverage==7.10.7",
              "termcolor==3.1.0",
              "pytest-cov==7.0.0",
              "Sphinx==7.4.7",
              "Werkzeug==3.0.6",
              "flask-security-too==4.1.6"
          ]
      },
      test_suite='nose.collector')

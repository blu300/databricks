# Filename: setup.py
from setuptools import setup, find_packages

import dabdemo

setup(
  name = "dabdemo",
  version = dabdemo.__version__,
  author = dabdemo.__author__,
  url = "https://<my-url>",
  author_email = "benjaminbluestone@kubrickgroup.com",
  description = "I have no idea what I'm doing.",
  packages = find_packages(include = ["dabdemo"]),
  entry_points={"group_1": "run=dabdemo.__main__:main"},
  install_requires = ["setuptools"]
)
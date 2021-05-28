from setuptools import setup, find_packages

PYTHON_REQUIRES = ">=3.6"

setup(name='laminage',
      version='1.1',
      python_requires=PYTHON_REQUIRES,
      include_package_data=True,
      packages=find_packages(),
      )
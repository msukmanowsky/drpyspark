import re
from setuptools import setup, find_packages

def get_version():
    '''Get version without importing, which avoids dependency issues'''
    with open('drpyspark/version.py') as version_file:
        return re.search(r"""__version__\s+=\s+(['"])(?P<version>.+?)\1""",
                         version_file.read()).group('version')


def readme():
    with open('README.md') as f:
        return f.read()


install_requires = []
tests_require = []
setup_requires = []
lint_requires = []
setup(
    name='drpyspark',
    version=get_version(),
    author='Mike Sukmanowsky',
    author_email='mike.sukmanowsky@gmail.com',
    url='https://github.com/msukmanowsky/drpyspark',
    description=('drpyspark provides handy utilities for debugging and tuning '
                 'Apache Spark programs (specifically, pyspark).'),
    long_description=readme(),
    license='Apache License 2.0',
    packages=find_packages(),
    install_requires=install_requires,
    tests_require=tests_require,
    setup_requires=setup_requires,
)

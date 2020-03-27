from setuptools import setup

VERSION = '0.0.1'

NAME = 'insider_trading'

setup(
    name=NAME,
    version=VERSION,
    description='',
    url='',
    author='Yuliya Karanouskaya',
    author_email='y.karanouskaya@gmail.com',
    install_requires=[
        "beautifulsoup4",
    ],
    scripts=[
            "bin/update-database",
            "bin/update-market-data",
            "bin/merge-data"
    ]
)
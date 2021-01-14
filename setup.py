from setuptools import setup, find_packages


with open('README.txt', 'r', encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='sql_db',
    version='0.0.1',
    packages=find_packages(),
    install_requires=['pyodbc', 'pandas >= 1.0.3', 'sqlalchemy >= 1.3.13', 'six >= 1.14.0', 'psycopg2 >= 2.8.5'],
    author='Yaroslav Khnykov',
    description='Simple module to handle with databases',
    long_description=long_description
)

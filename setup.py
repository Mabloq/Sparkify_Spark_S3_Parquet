from setuptools import setup, find_packages

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    name='setup_test',
    version='1.0',
    packages=find_packages(),
    install_requires=requirements,
    url='https://github.com/Mabloq/setup-test.git',
    license='MIT',
    author='matthewarcila',
    author_email='',
    description='testing setuptools lol'
)

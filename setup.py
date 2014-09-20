from setuptools import setup, find_packages
from pip.req import parse_requirements

reqs = [str(ir.req) for ir in parse_requirements('requirements.txt')]

descr = 'A dynamic REST interface for NoSQL datastores'

setup(
    name='endless',
    description=descr,
    long_description=descr,
    version='0.1',
    author='Brad Willard',
    author_email='info@bradwillard.com',
    url='http://www.github.com/brdw/endless',
    packages=find_packages(),
    install_requires=reqs,
    include_package_data=True,
    license='GNU GPL v3'
)

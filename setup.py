import versioneer
from distutils.core import setup
from setuptools import find_packages

packages = find_packages()
packages.remove('sparsity.test')

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name='sparsity',
    version=versioneer.get_version(),
    author='Alan Hoeng',
    author_email='alan.f.hoeng@gmail.com',
    description="Sparse data processing toolbox",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/datarevenue-berlin/sparsity",
    packages=packages,
    cmdclass=versioneer.get_cmdclass(),
    install_requires=[
        'pandas>=0.19.0,<=0.23.4',
        'scipy>=0.18.1',
        'numpy>=1.12.0',
        's3fs>=0.1.0',
        'dask<=0.19.2'
    ],
    test_requires=[
        'moto',
    ],
    zip_safe=False,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
    ],
)

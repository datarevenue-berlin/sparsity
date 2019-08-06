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
        'pandas>=0.21.0,<=0.25.0',
        'scipy>0.19.1',
        'numpy>=1.12.0',
        's3fs>=0.1.0',
        'dask>=2.1.0',
        'fsspec>=0.3.3',
    ],
    test_requires=[
        'boto3==1.7.84',
        'botocore==1.10.84',
        'moto==1.3.6'
    ],
    zip_safe=False,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
    ],
)

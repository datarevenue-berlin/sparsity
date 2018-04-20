import versioneer
from distutils.core import setup
from setuptools import find_packages

packages = find_packages()
packages.remove('sparsity.test')

setup(
    name='sparsity',
    version=versioneer.get_version(),
    author='Alan Hoeng',
    author_email='alan.f.hoeng@gmail.com',
    packages=packages,
    cmdclass=versioneer.get_cmdclass(),
    install_requires=[
                        'pandas>=0.19.0',
                        'scipy>=0.18.1',
                        'numpy>=1.12.0',
                        's3fs>=0.1.0'
                    ],
    test_requires=[
        'moto'
    ],
    zip_safe=False
)
from setuptools import setup, find_packages

setup(
    name='land_registry_processor',
    version='0.1',
    packages=find_packages(),
    install_requires=[
        'dask[dataframe]',
        'pandas',
    ],
    entry_points={
        'console_scripts': [
            'land_registry_processor = land_registry_processor.main:main',
        ],
    },
)
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages
from pathlib import Path


with open('README.md') as f:
    readme = f.read()

PACKAGE_NAME = 'aodn_cloud_optimised'

INSTALL_REQUIRES = [
    'netCDF4',
    'numpy',
    'pandas',
    'python-dateutil',
    'jsonschema',
    'h5py',
    'scipy',
    'boto3',
    'pyarrow==15.0.2',
    'rechunker',
    's3fs',
    'shapely',
    'xarray[complete]',
    'zarr'
]

PACKAGE_DATA = {
    'aodn_cloud_optimised.config': ['*.json'],
    'aodn_cloud_optimised.config.dataset': ['*.json'],

}

SCRIPTS = [
    'aodn_cloud_optimised/bin/aatams_acoustic_tagging.py',
    'aodn_cloud_optimised/bin/acorn_gridded_qc_turq.py',
    'aodn_cloud_optimised/bin/anfog_to_parquet.py',
    'aodn_cloud_optimised/bin/anmn_ctd_to_parquet.py',
    'aodn_cloud_optimised/bin/anmn_aqualogger_to_parquet.py',
    'aodn_cloud_optimised/bin/ardc_wave_to_parquet.py',
    'aodn_cloud_optimised/bin/argo_to_parquet.py',
    'aodn_cloud_optimised/bin/gsla_nrt_to_zarr.py',
    'aodn_cloud_optimised/bin/soop_xbt_nrt_to_parquet.py',
    'aodn_cloud_optimised/bin/srs_oc_ljco_to_parquet.py',
    'aodn_cloud_optimised/bin/srs_l3s_1d_dn_to_zarr.py'
]

PACKAGE_EXCLUDES = ['test*']

TESTS_REQUIRE = [
    'pytest',
    'ipython',
    'ipdb'
]

EXTRAS_REQUIRE = {
    'testing': TESTS_REQUIRE,
    'interactive': TESTS_REQUIRE
}

# Read the README file
README_PATH = Path(__file__).parent / 'README.md'
with README_PATH.open(encoding='utf-8') as f:
    readme = f.read()

setup(
    name=PACKAGE_NAME,
    version='0.1.0',
    description='AODN tool to create parquet dataset from NetCDF files',
    long_description=readme,
    long_description_content_type='text/markdown',
    author='Laurent Besnard',
    author_email='laurent.besnard@utas.edu.au',
    url='https://github.com/aodn/',
    install_requires=INSTALL_REQUIRES,
    packages=find_packages(exclude=PACKAGE_EXCLUDES),
    scripts=SCRIPTS,
    package_data=PACKAGE_DATA,
    test_suite='test_aodn_cloud_optimised',
    tests_require=TESTS_REQUIRE,
    extras_require=EXTRAS_REQUIRE,
    zip_safe=False,
    python_requires='==3.10.14',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: Implementation :: CPython',
    ]
)

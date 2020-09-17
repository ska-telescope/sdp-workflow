#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""PIP setup script for the SDP workflow package."""

import setuptools
import ska_sdp_workflow

with open("README.md", "r") as fh:
    LONG_DESCRIPTION = fh.read()

setuptools.setup(
    name='ska-sdp-workflow',
    version=ska_sdp_workflow.__version__,
    description='SKA SDP workflow library',
    author='SKA Sim Team',
    license='License :: OSI Approved :: BSD License',
    long_description=LONG_DESCRIPTION,
    long_description_content_type='text/markdown',
    url='https://gitlab.com/ska-telescope/sdp-workflow',
    packages=setuptools.find_packages(),
    # Workaround: avoid declaring pytango dependency.
    # It's ok to fail to load if not there.
    # install_requires=[
    #     'pytango'
    # ],
    setup_requires=['pytest-runner'],
    tests_require=[
        'pylint2junit',
        'pytest',
        'pytest-cov',
        'pytest-json-report',
        'pytest-pycodestyle',
        'pytest-pydocstyle',
        'pytest-pylint'
    ],
    zip_safe=False,
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Environment :: Console",
        "Intended Audience :: Science/Research",
        "Topic :: Scientific/Engineering :: Astronomy",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python :: 3 :: Only",
        "License :: OSI Approved :: BSD License"
    ]
)

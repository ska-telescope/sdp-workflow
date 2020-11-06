#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""PIP setup script for the SDP workflow package."""

import os
import setuptools

RELEASE_INFO = {}
RELEASE_PATH = os.path.join('src', 'ska_sdp_workflow', 'release.py')
exec(open(RELEASE_PATH).read(), RELEASE_INFO)

with open("README.md", "r") as fh:
    LONG_DESCRIPTION = fh.read()

setuptools.setup(
    name=RELEASE_INFO['NAME'],
    version=RELEASE_INFO['VERSION'],
    description='SKA SDP Workflow',
    author=RELEASE_INFO['AUTHOR'],
    license=RELEASE_INFO['LICENSE'],
    long_description=LONG_DESCRIPTION,
    long_description_content_type='text/markdown',
    url='https://gitlab.com/ska-telescope/sdp-workflow',
    # packages=setuptools.find_packages(),
    package_dir={"": "src"},
    packages=setuptools.find_namespace_packages(where="src"),
    # Workaround: avoid declaring pytango dependency.
    # It's ok to fail to load if not there.
    install_requires=[
        'ska_logging >= 0.3'
        'ska-sdp-config>=0.0.11',
        'ska-sdp-logging>=0.0.6',
        'distributed==2.30.0'
    ],
    setup_requires=['pytest-runner'],
    tests_require=[
        'pylint2junit',
        'pytest',
        'pytest-cov',
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

#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""PIP setup script for the SDP workflow package."""

import setuptools

with open("README.md", "r") as fh:
    LONG_DESCRIPTION = fh.read()

version = {}
with open('src/ska_sdp_workflow/version.py', 'r') as file:
    exec(file.read(), version)

setuptools.setup(
    name='ska-sdp-workflow',
    version=version['__version__'],
    description='SKA SDP Workflow',
    author='Sim Team',
    license='License :: OSI Approved :: BSD License',
    long_description=LONG_DESCRIPTION,
    long_description_content_type='text/markdown',
    url='https://gitlab.com/ska-telescope/sdp-workflow',
    # packages=setuptools.find_packages(),
    package_dir={"": "src"},
    packages=setuptools.find_namespace_packages(where="src"),
    # Workaround: avoid declaring pytango dependency.
    # It's ok to fail to load if not there.
    install_requires=[
        'ska_logging >= 0.3',
        'ska-sdp-config>=0.0.11',
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

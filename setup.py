#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""PIP setup script for the SDP workflow package."""

# pylint: disable=exec-used

import os
import setuptools

with open("README.md", "r") as fh:
    LONG_DESCRIPTION = fh.read()

version = {}
VERSION_PATH = os.path.join("src", "ska_sdp_workflow", "version.py")
with open(VERSION_PATH, "r") as file:
    exec(file.read(), version)


def requirements_from(fname):
    """ Read requirements from a file. """
    with open(fname) as req_file:
        return [req for req in req_file.read().splitlines() if req[0] != "-"]


setuptools.setup(
    name="ska-sdp-workflow",
    version=version["__version__"],
    description="SKA SDP Workflow",
    author="Sim Team",
    license="License :: OSI Approved :: BSD License",
    long_description=LONG_DESCRIPTION,
    long_description_content_type="text/markdown",
    url="https://gitlab.com/ska-telescope/sdp/ska-sdp-workflow",
    package_dir={"": "src"},
    packages=setuptools.find_packages("src"),
    install_requires=requirements_from("requirements.txt"),
    setup_requires=["pytest-runner"],
    tests_require=requirements_from("requirements-test.txt"),
    zip_safe=False,
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Environment :: Console",
        "Intended Audience :: Science/Research",
        "Topic :: Scientific/Engineering :: Astronomy",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python :: 3 :: Only",
        "License :: OSI Approved :: BSD License",
    ],
)

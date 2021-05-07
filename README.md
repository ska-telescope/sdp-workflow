# SDP Workflow Library

## Introduction
The SDP workflow library is a high-level interface for writing workflows. Its
goal is to provide abstractions to enable the developer to express the
high-level organisation of a workflow without needing to interact directly with
the low-level interfaces such as the SDP configuration library.


## Contribute to this repository
We use [Black](https://github.com/psf/black) to keep the python code style in good shape. 

Please make sure you black-formatted your code before merging to master.

The first step in the CI pipeline checks that the code complies with black formatting style,

and will fail if that is not the case.


## Releasing the python package

When new release is ready:

  - check out master
  - update CHANGELOG.md
  - update setup.cfg [bumpver]: tag = True
  - commit changes
  - make release-[patch||minor||major]
  - update setup.cfg [bumpver]: tag = False
  - commit changes
  - make patch-beta

Note: bumpver needs to be installed

The CI pipeline will trigger on every push, but will only publish artifacts when "tag = True" in setup.cfg.
    


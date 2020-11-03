SDP Workflow Library
====================

Introduction
------------

The workflow library is intended to provide a high-level interface for
writing workflows. The goal is to abstract away away all direct interactions
with the configuration database, and especially deployments so workflows will no
longer need to use the low-level interfaces directly.


Functionality
-------------

Starting, monitoring and ending a workflow.
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- At the start

  - Claim processing block.
  - Get parameters - they should be checked against the parameter schema
    defined for the workflow.

- Buffer Request

  - Requests for input and output buffer space. The workflow will calculate the
    resources it needs based on the parameters, then request them from the processing controller.
    This is currently a placeholder.

- Create Phase

  - Declare phases. In the current implementation, only one phase is declared the work phase.

- Start Work Phase

  - Entering this phase, it checks if the resources are available and waits until it is and internally
    checks whether the processing block was cancelled. For real-time workflow, it checks if the scheduling
    block instance was cancelled.
  - Deploys execution engine
  - Executes a script/function using the execution engine
  - Monitors the execution engine and processing block state. Waits until its either finished or cancelled

- At the end

  - Removes the execution engine
  - Release resources.
  - Update processing block state with information about the success or failure of the workflow

Receive workflows
^^^^^^^^^^^^^^^^^

 - Get IP and MAC addresses for the receive processes.
 - Monitor receive processes. If any get restarted, then the addresses may need to be updated.
 - Write the addresses in the appropriate format into the processing block state.


Installation
------------

The library can be installed using pip but need to make to set Nexus as the source for pip:

    .. code-block::

       pip install -i https://nexus.engageska-portugal.pt/repository/pypi/simple ska-telescope-model

To install it through the requirements.txt file, then add the source to the file:

    .. code-block::

      --index-url https://nexus.engageska-portugal.pt/repository/pypi/simple
      --extra-index-url https://pypi.org/simple

Usage
-----

Once the SDP workflow library have been installed, use:

    .. code-block::

       from ska_sdp_workflow import ProcessingBlock


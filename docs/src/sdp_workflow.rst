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

- At the start

  - Claim processing block.
  - Get parameters - they should be checked against the parameter schema
    defined for the workflow.

- Buffer Request

  - Requests for input and output buffer space. The workflow will calculate the
    resources it needs based on the parameters, then request them from the processing controller.
    This is currently a placeholder.

- Create Phase

  -

- Start Work Phase

  -

Monitor scheduling block instance (for real-time workflows)
A real-time workflow needs to respond to changes in the SBI resulting from commands on the subarray device. Most importantly it needs to know when the SBI has been ended so it can clean up and exit.
Monitor processing block state
Workflow should clean up and exit if requested/forced by processing controller.
At the end
Release resources.
Update processing block state with information about the success or failure of the workflow

Receive workflows

 - Get IP and MAC addresses for the receive processes.
 - Monitor receive processes. If any get restarted, then the addresses may need to be updated.
 - Write the addresses in the appropriate format into the processing block state.

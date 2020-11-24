Introduction
============

Functionality
-------------

The required functionality of the workflow library is as follows.

Starting, monitoring and ending a workflow
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- At the start

  - Claim the processing block.
  - Get the parameters defined in the processing block. They should be checked
    against the parameter schema defined for the workflow.

- Resource requests

  - Make requests for input and output buffer space. The workflow will
    calculate the resources it needs based on the parameters, then request them
    from the processing controller. This is currently a placeholder.

- Declare workflow phases

  - Workflows will be divided into phases such as preparation, processing,
    and clean-up. In the current implementation, only one phase can be
    declared, which we refer to as the 'work' phase.

- Execute the work phase

  - On entry to the work phase, it waits until the resources are available.
    Meanwhile it monitors the processing block to see it has been cancelled.
    For real-time workflows, it also checks if the scheduling block instance
    has been cancelled.
  - Deploy execution engines to execute a script/function.
  - Monitor the execution engines and processing block state. Waits until the
    execution is finished, or the processing block is cancelled.

- At the end

  - Remove the execution engines to release the resources.
  - Update processing block state with information about the success or failure
    of the workflow.

Receive workflows
^^^^^^^^^^^^^^^^^

 - Get IP and MAC addresses for the receive processes.
 - Monitor receive processes. If any get restarted, then the addresses may need to be updated.
 - Write the addresses in the appropriate format into the processing block state.


Installation
------------

The library can be installed using ``pip`` but you need to make sure to use the
EngageSKA Nexus repository as the index:

.. code-block::

  pip install \
    --index-url https://nexus.engageska-portugal.pt/repository/pypi/simple \
    --extra-index-url https://pypi.org/simple \
    ska-sdp-workflow

To install it using a ``requirements.txt`` file, the ``pip`` options can be
added to the top of the file like this:

.. code-block::

  --index-url https://nexus.engageska-portugal.pt/repository/pypi/simple
  --extra-index-url https://pypi.org/simple
  ska-sdp-workflow

Usage
-----

Once the SDP workflow library have been installed, use:

.. code-block::

  import ska_sdp_workflow

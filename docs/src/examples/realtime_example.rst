Real-time workflow
==================

This is a simple example of a real-time workflow to show how to use the
workflow library.

This example requires the  ``logging``, ``ska.logging`` and
``ska_sdp_workflow`` Python modules.

.. code-block::

  import logging
  import ska.logging
  import ska_sdp_workflow

We initialise the logging

.. code-block::

  ska.logging.configure_logging()
  LOG = logging.getLogger('test_realtime')
  LOG.setLevel(logging.DEBUG)

The first step is to claim the processing block and get its parameters.

.. code-block::

  pb = workflow.ProcessingBlock()
  parameters = pb.get_parameters()

We then need to request the input and output buffer reservations. This is a
just placeholder currently to give an example of how resources will be
requested.

.. code-block::

  in_buffer_res = pb.request_buffer(100e6, tags=['sdm'])
  out_buffer_res = pb.request_buffer(parameters['length'] * 6e15 / 3600, tags=['visibilities'])

We declare the phases of the workflow and define which resource requests are
needed for each phase. In the current implementation, we can only declare one
phase, which in this example we call the 'work' phase:

.. code-block::

  work_phase = pb.create_phase('Work', [in_buffer_res, out_buffer_res])

We start the work phase using a ``with`` block. On entry, it waits until the
resources are available and in the meantime it monitors the processing block
state and scehduling block instance to check if it has been cancelled. Once the
resources are available, it proceeds into the body of the ``with`` block and
deploys a Helm chart using the ``ee_deploy_helm`` method.

.. code-block::

  with work_phase:
      work_phase.ee_deploy_helm('cbf-sdp-emulator')

On exit from the ``with`` block it waits until the scheduling block instance is
finished or cancelled. When the scheduling block instance state is updated, it
then removes the execution engine and updates processing block state.

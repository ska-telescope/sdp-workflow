Batch workflow
==============

This is a simple example of a batch workflow to show how to use the workflow
library.

This example requires the ``time``, ``logging``, ``ska.logging`` and
``ska_sdp_workflow`` Python modules:

.. code-block::

  import time
  import logging
  import ska.logging
  import ska_sdp_workflow

We initialise the logging:

.. code-block::

  ska.logging.configure_logging()
  LOG = logging.getLogger('test_batch')
  LOG.setLevel(logging.DEBUG)

The first step is to claim the processing block and get its parameters.

.. code-block::

  pb = ska_sdp_workflow.ProcessingBlock()
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

    work_phase = pb.create_phase('work', [in_buffer_res, out_buffer_res])

Next, we define the function to be executed by the execution engine. In a real
workflow this would most likely be imported from an library of standard
workflow functions.

.. code-block::

  def some_processing(duration: float):
      """
      Do some 'processing' for the required duration.
      """
      LOG.info('Starting processing for %f s', duration)
      time.sleep(duration)
      LOG.info('Finished processing')

We start the work phase using a ``with`` block. On entry, it waits until the
resources are available and in the meantime it monitors the processing block
state to check if it has been cancelled. Once the resources are available, it
proceeds into the body of the ``with`` block and deploys a fake execution
engine using the ``ee_deploy_test`` method to execute our function. The example
we defined sleeps for the specified duration. This happens in a separate
thread, so the method returns immediately. It then enters the loop at the end
which waits until the execution is finished, and also monitors the processing
block state. On exiting the ``with`` block it removes the execution engine
deployment and updates the processing block state with the status of the
workflow.

.. code-block::

  with work_phase:

      deploy = work_phase.ee_deploy_test(
          'test', some_processing, (parameters['duration'],)
      )

      for txn in work_phase.wait_loop():
          if deploy.is_finished(txn):
              break

Simple Batch Workflow Example
=============================

This is a simple batch workflow example to show basic usage of the workflow library.

We require ``time``, ``logging``, ``ska.logging`` and ``ska_sdp_workflow`` Python modules.

  .. code-block::

    import time
    import logging
    import ska.logging

    from ska_sdp_workflow import workflow

We then need to initialise logging

  .. code-block::

    ska.logging.configure_logging()
    LOG = logging.getLogger('test_batch')
    LOG.setLevel(logging.DEBUG)


At first we need to claim processing block and get parameters.

  .. code-block::

    pb = workflow.ProcessingBlock()
    parameters = pb.get_parameters()

We then need to request for input and output buffer space. Currently this method doesn't do any
calculation for resources based on the parameters. This is just a placeholder.

  .. code-block::

    in_buffer_res = pb.request_buffer(100e6, tags=['sdm'])
    out_buffer_res = pb.request_buffer(parameters['length'] * 6e15 / 3600, tags=['visibilities'])


Next, we declare phases. In the current implementation, we only declare one phase which is the work phase.

  .. code-block::

     work_phase = pb.create_phase('Work', [in_buffer_res, out_buffer_res])


Then, we go and start the work phase. Before entering, it waits until the resources are available and checks
internally whether the processing block was cancelled.Once the resources are available, it deploys the execution engine
using the ``ee_deploy`` method with a function to execute and returns a handle immediately. In this example, the function
sleeps for a certain duration.

  .. code-block::

    def some_processing(duration):
        """Doing some processing for the required duration"""
        LOG.info('Starting processing for %f s', duration)
        time.sleep(duration)
        LOG.info('Finished processing')

    with work_phase:

       deploy = work_phase.ee_deploy(func=some_processing,
                                     f_args=((parameters['duration'],)))

While its doing some processing, execution engine and processing block state is monitored and waits until its either
finished or cancelled.

  .. code-block::

    while not deploy.is_finished:
        pass

Once the deployment is finished, it then removes the execution engine and updates processing block state.
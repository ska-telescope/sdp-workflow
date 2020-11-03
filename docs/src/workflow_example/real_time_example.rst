Simple Real-Time Workflow Example
=================================

This is a simple real-time workflow example to show basic usage of the workflow library.

We require ``logging``, ``ska.logging`` and ``ska_sdp_workflow`` Python modules.

  .. code-block::

    import logging
    import ska.logging

    from ska_sdp_workflow import workflow

We then need to initialise logging

  .. code-block::

    ska.logging.configure_logging()
    LOG = logging.getLogger('test_realtime')
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
internally whether the processing block and the scheduling block instance was cancelled.

Once the resources are available, it deploys the execution engine using the ``ee_deploy`` method with the
deployment chart name as a parameter.

  .. code-block::

    with work_phase:
        work_phase.ee_deploy('cbf-sdp-emulator')

Once the deployment is finished, it exits from the ``work_phase`` and waits until the scheduling block instance is set finished or cancelled.
When the scheduling block instance state is updated, it then removes the execution engine and updates processing block state.
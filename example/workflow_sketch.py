import ska_sdp_workflow as workflow
import logging

logger = logging.getLogger(__name__)
pb = workflow.claim_processing_block()

# If we want parameters schema checking (here using "schema" Python library)
# Can normalise data at the same time, so it's a good idea to pipe data through
from schema import Schema

par_schema = {'str-par': str, 'length': float}
parameters = pb.get_parameters(par_schema)

# .validate(pb.parameters)

# Request buffer space. Tag roughly what this is for
# so that the platform might use particular storage backends for data
in_buffer_res = pb.request_buffer(100e6, tags=['sdm'])
out_buffer_res = pb.request_buffer(parameters['length'] * 6e15 / 3600, tags=['visibilities'])

# For real-time processing a certain compute reservation should have been
# assigned to the subarray already.
compute_res = pb.request_compute([compute_phase], pb.subarray.allocated_compute())

# Declare phases. For batch processing, these would need hints about minimum and
# maximum time the phases are expected to take.
# prep_phase = pb.create_phase('Preparation', [in_buffer_res])
compute_phase = pb.create_phase('Work', [in_buffer_res, out_buffer_res, compute_res])
finish_phase = pb.create_phase('Finish', [in_buffer_res, out_buffer_res])

# # Start preparation phase. Entering the first phase implicitly means that the workflow
# # is finished declaring phases & resources, and therefore ready to be scheduled.
# # Attempting to declare any more should be an error.
# with prep_phase:
#     sdm_stage = pb.create_science_data_model({'sdm': in_buffer_res})
#
#     # Wait for sky model stage to finish
#     # The "wait_loop" checks internally whether the processing block was cancelled,
#     # possibly does some standard state propagation in the configuration database
#     # (e.g. checks deployments) and otherwise waits after the iteration (i.e. loop(wait=True))
#     for txn in pb.wait_loop():
#         # "Finished" should mean successful finish, error or cancellation
#         if sdm_stage.is_finished(txn):
#             break
#
#         # Check that sky model phase finished successfully. The script could decide to re-try here.
#         if sdm_stage.is_error(txn):
#             pb.set_error("Could not create science data model!")
#             exit(1)

# Start work phase
with compute_phase:
    # Create execution engine. Data island is created implicitly by given buffer resources.
    # TODO: Island parallelism.
    compute_stage = pb.deploy(workflow.create_ee(...), {
        'sdm': in_buffer_res, 'visibilities': out_buffer_res
    })

    # Wait for stage to finish or subarray to be done
    for txn in pb.wait_loop():
        if compute_stage.is_finished(txn):
            break
        # if not pb.subarray.is_configured(txn):
        #     break

        if compute_stage.is_error(txn):
            pb.set_error(txn, "Could not do real-time processing!")
            exit(1)

    # Execution engine should be torn down automatically at the end of this phase

# Start finish phase
with finish_phase:
    # Register data products etcetera
    delivery_stage = pb.start_delivery({
        'sdm': in_buffer_res, 'visibilities': out_buffer_res
    })

pb.finish("Success!")
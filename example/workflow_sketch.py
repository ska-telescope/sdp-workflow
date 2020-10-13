import ska_sdp_workflow as workflow
import logging

logger = logging.getLogger(__name__)
pb = workflow.claim_processing_block()

# If we want parameters schema checking (here using "schema" Python library)
# Can normalise data at the same time, so it's a good idea to pipe data through
from schema import Schema

par_schema = {'str-par': str, 'length': float}
parameters = par_schema.validate(pb.parameters)

# Request buffer space. Tag roughly what this is for
# so that the platform might use particular storage backends for data
in_buffer_res = pb.request_buffer(100e6, tags=['sdm'])
out_buffer_res = pb.request_buffer(parameters['length'] * 6e15 / 3600, tags=['visibilities'])

# For real-time processing a certain compute reservation should have been
# assigned to the subarray already.
compute_res = pb.request_compute([compute_phase], pb.subarray.allocated_compute())


# work_phase will only be implemented in this phase
work_phase = pb.create_phase('Work', [in_buffer_res, out_buffer_res, compute_res])

# Start work phase
# Inside work_phase, requires a wait loop to check if the pb is cancelled or sbi is finished or cancelled and
# to check if the resources are avaliable
with work_phase:
    # Create execution engine. Data island is created implicitly by given buffer resources.
    # TODO: Island parallelism.
    deploy_id = 'proc-{}-vis-receive'.format(pb.id)
    compute_stage = pb.deploy(
        deploy_id, "helm", {
            'chart': 'vis-receive',  # Helm chart deploy/charts/vis-receive
        })

    # Wait for stage to finish or subarray to be done
    # wait-loop should be checking if the pb is cancelled or lost ownership
    for txn in pb.wait_loop():
        # Checks
        if compute_stage.is_finished(txn):
            break

        # For real-time
        if pb.is_finished(txn):
            break

        if compute_stage.is_error(txn):
            pb.exit(txn, "Could not do real-time processing!")
            exit(1)


    # Execution engine should be torn down automatically at the end of this phase

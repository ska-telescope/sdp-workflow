"""SDP Workflow tests."""

# pylint: disable=redefined-outer-name
# pylint: disable=duplicate-code

import logging
from unittest.mock import patch

import ska_sdp_config
import logging
from ska_sdp_workflow import workflow

# LOG = logging.getLogger(__name__)

LOG = logging.getLogger('worklow-test')
LOG.setLevel(logging.DEBUG)

PREFIX = "/__test_pb"

WORKFLOW = {
    'type': 'realtime',
    'id': 'test_rt_workflow',
    'version': '0.0.1'
}


def wipe_db(config):
    config.backend.delete('/pb', must_exist=False, recursive=True)


def test_claim_processing_block():
    """Test Claiming processing block"""

    config = ska_sdp_config.Config()
    wipe_db(config)

    for txn in config.txn():
        pb1_id = txn.new_processing_block_id('test')
        pb1 = ska_sdp_config.ProcessingBlock(
            pb1_id,
            sbi_id='sbi-test',
            parameters={},
            dependencies=[],
            workflow=WORKFLOW)

        assert txn.get_processing_block(pb1_id) is None
        txn.create_processing_block(pb1)
        assert txn.get_processing_block(pb1_id).id == pb1_id

    pb = workflow.ProcessingBlock(pb1_id)

    # Need to sort this out
    for txn in config.txn():
        assert txn.get_processing_block_owner(pb1_id) == config.owner
        assert txn.is_processing_block_owner(pb1_id)


def test_input_buffer_request():
    """Test requesting input uffer."""

    config = ska_sdp_config.Config()
    wipe_db(config)

    for txn in config.txn():
        pb1_id = txn.new_processing_block_id('test')
        pb1 = ska_sdp_config.ProcessingBlock(
            pb1_id,
            sbi_id='sbi-test',
            parameters={},
            dependencies=[],
            workflow=WORKFLOW)

        txn.create_processing_block(pb1)

    pb = workflow.ProcessingBlock(pb1_id)
    in_buffer_res = pb.request_buffer(100e6, tags=['sdm'])
    assert in_buffer_res is not None


def test_output_buffer_request():
    """Test requesting output buffer."""
    config = ska_sdp_config.Config()
    wipe_db(config)

    for txn in config.txn():
        pb1_id = txn.new_processing_block_id('test')
        pb1 = ska_sdp_config.ProcessingBlock(
            pb1_id,
            sbi_id='sbi-test',
            parameters={"length": 10},
            dependencies=[],
            workflow=WORKFLOW)

        txn.create_processing_block(pb1)

    pb = workflow.ProcessingBlock(pb1_id)
    parameters = pb.get_parameters()
    assert parameters['length'] == 10
    out_buffer_res = pb.request_buffer(parameters['length'] * 6e15 / 3600, tags=['visibilities'])
    assert out_buffer_res is not None


def test_create_phase():
    """Test creating work phase."""
    config = ska_sdp_config.Config()
    wipe_db(config)

    for txn in config.txn():
        pb1_id = txn.new_processing_block_id('test')
        pb1 = ska_sdp_config.ProcessingBlock(
            pb1_id,
            sbi_id='sbi-test',
            parameters={"length": 10},
            dependencies=[],
            workflow=WORKFLOW)

        txn.create_processing_block(pb1)

    pb = workflow.ProcessingBlock(pb1_id)
    parameters = pb.get_parameters()
    in_buffer_res = pb.request_buffer(100e6, tags=['sdm'])
    out_buffer_res = pb.request_buffer(parameters['length'] * 6e15 / 3600, tags=['visibilities'])

    # work_phase = pb.create_phase('Work', [in_buffer_res, out_buffer_res])

    #
    # with work_phase:
    #     # Create execution engine. Data island is created implicitly by given buffer resources.
    #     # TODO: Island parallelism.
    #
    #     sbi = pb.sbi().is_finished()
    #     assert sbi == 1

        # deploy_id = 'proc-{}-vis-receive'.format(pb.id)
        # chart = {'chart': 'vis-receive',}
        # compute_stage = pb.deploy(
        #     deploy_id, "helm", chart)
#
#         assert compute_stage is not None
#
#         # # Wait for stage to finish or subarray to be done
#         # # wait-loop should be checking if the pb is cancelled or lost ownership
#         # for txn in pb.wait_loop():
#         #     # Checks
#         #     if compute_stage.is_finished(txn):
#         #         break
#         #
#         #     # For real-time
#         #     if pb.is_finished(txn):
#         #         break
#         #
#         #     if compute_stage.is_error(txn):
#         #         pb.exit(txn, "Could not do real-time processing!")
#         #         exit(1)
#
#
# # def test_deploy_fail():
# #     """Test deploy execution engine failed."""
#
#

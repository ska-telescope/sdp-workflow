"""SDP Workflow library tests."""

# pylint: disable=redefined-outer-name
# pylint: disable=duplicate-code
# pylint: disable=invalid-name

import os
import json
import logging
from unittest.mock import patch
import ska_sdp_config

from ska_telmodel.schema import validate
from ska_telmodel.sdp.version import SDP_RECVADDRS
from ska_sdp_workflow import workflow

LOG = logging.getLogger("workflow-test")
LOG.setLevel(logging.DEBUG)

CONFIG_DB_CLIENT = workflow.new_config_db()
SUBARRAY_ID = "01"
MOCK_ENV_VARS = {"SDP_HELM_NAMESPACE": "sdp"}
SCHEMA_VERSION = "0.2"
SCAN_TYPES = [
    {
        "id": "science_A",
        "coordinate_system": "ICRS",
        "ra": "02:42:40.771",
        "dec": "-00:00:47.84",
        "channels": [
            {
                "count": 744,
                "start": 0,
                "stride": 2,
                "freq_min": 0.35e9,
                "freq_max": 0.368e9,
                "link_map": [[0, 0], [200, 1], [744, 2], [944, 3]],
            },
            {
                "count": 744,
                "start": 2000,
                "stride": 1,
                "freq_min": 0.36e9,
                "freq_max": 0.368e9,
                "link_map": [[2000, 4], [2200, 5]],
            },
        ],
    },
    {
        "id": "calibration_B",
        "coordinate_system": "ICRS",
        "ra": "12:29:06.699",
        "dec": "02:03:08.598",
        "channels": [
            {
                "count": 744,
                "start": 0,
                "stride": 2,
                "freq_min": 0.35e9,
                "freq_max": 0.368e9,
                "link_map": [[0, 0], [200, 1], [744, 2], [944, 3]],
            },
            {
                "count": 744,
                "start": 2000,
                "stride": 1,
                "freq_min": 0.36e9,
                "freq_max": 0.368e9,
                "link_map": [[2000, 4], [2200, 5]],
            },
        ],
    },
]

RECV_ADDRESS = {
    "interface": "https://schema.skatelescope.org/ska-sdp-recvaddrs/0.2",
    "calibration_B": {
        "host": [
            [
                0,
                "proc-pb-mvp01-20200425-00000-test-receive-0.receive.sdp.svc.cluster.local",
            ],
            [
                2000,
                "proc-pb-mvp01-20200425-00000-test-receive-0.receive.sdp.svc.cluster.local",
            ],
        ],
        "port": [[0, 9000, 1], [2000, 9000, 1]],
    },
    "science_A": {
        "host": [
            [
                0,
                "proc-pb-mvp01-20200425-00000-test-receive-0.receive.sdp.svc.cluster.local",
            ],
            [
                2000,
                "proc-pb-mvp01-20200425-00000-test-receive-0.receive.sdp.svc.cluster.local",
            ],
        ],
        "port": [[0, 9000, 1], [2000, 9000, 1]],
    },
}


def test_claim_processing_block():
    """Test claiming processing block"""

    # Wipe the config DB
    wipe_config_db()

    # Create sbi and pb
    create_sbi_pbi()

    for txn in CONFIG_DB_CLIENT.txn():
        pb_list = txn.list_processing_blocks()
        for pb_id in pb_list:
            assert txn.get_processing_block(pb_id).id == pb_id
            workflow.ProcessingBlock(pb_id)
            assert txn.is_processing_block_owner(pb_id)


def test_buffer_request():
    """Test requesting input and output buffer."""

    # Wipe the config DB
    wipe_config_db()

    # Create sbi and pb
    create_sbi_pbi()

    for txn in CONFIG_DB_CLIENT.txn():
        pb_list = txn.list_processing_blocks()
        for pb_id in pb_list:
            pb = workflow.ProcessingBlock(pb_id)
            parameters = pb.get_parameters()
            assert parameters["length"] == 10
            in_buffer_res = pb.request_buffer(100e6, tags=["sdm"])
            out_buffer_res = pb.request_buffer(
                parameters["length"] * 6e15 / 3600, tags=["visibilities"]
            )
            assert in_buffer_res is not None
            assert out_buffer_res is not None


def test_real_time_workflow():
    """Test real time workflow."""

    # Wipe the config DB
    wipe_config_db()

    # Create sbi and pb
    create_sbi_pbi()

    # Create processing block states
    create_pb_states()

    pb_id = "pb-mvp01-20200425-00001"
    deploy_name = "cbf-sdp-emulator"
    deploy_id = "proc-{}-{}".format(pb_id, deploy_name)
    work_phase = create_work_phase(pb_id)

    with work_phase:
        for txn in CONFIG_DB_CLIENT.txn():
            sbi_list = txn.list_scheduling_blocks()
            for sbi_id in sbi_list:
                sbi = txn.get_scheduling_block(sbi_id)
                status = sbi.get("status")
                assert status == "ACTIVE"

                pb_state = txn.get_processing_block_state(pb_id)
                pb_status = pb_state.get("status")
                assert pb_status == "RUNNING"

                work_phase.ee_deploy_helm(deploy_name)
                deployment_list = txn.list_deployments()
                assert deploy_id in deployment_list

                # Set scheduling block instance to FINISHED
                sbi = {"subarray_id": None, "status": "FINISHED"}
                sbi_state = txn.get_scheduling_block(sbi_id)
                sbi_state.update(sbi)
                txn.update_scheduling_block(sbi_id, sbi_state)

                sbi = txn.get_scheduling_block(sbi_id)
                status = sbi.get("status")
                assert status == "FINISHED"

    for txn in CONFIG_DB_CLIENT.txn():
        pb_state = txn.get_processing_block_state(pb_id)
        pb_status = pb_state.get("status")
        assert pb_status == "FINISHED"


@patch.dict(os.environ, MOCK_ENV_VARS)
def test_batch_workflow():
    """Test batch workflow"""

    def calc(x, y):
        x1 = x
        y1 = y
        z = x1 + y1
        return z

    # Wipe the config DB
    wipe_config_db()

    # Create sbi and pb
    create_sbi_pbi()

    # Create processing block states
    create_pb_states()

    pb_id = "pb-mvp01-20200425-00002"
    deploy_name = "dask"
    # deploy_id = 'proc-{}-{}'.format(pb_id, deploy_name)
    n_workers = 2
    work_phase = create_work_phase(pb_id)

    with work_phase:
        for txn in CONFIG_DB_CLIENT.txn():
            pb_state = txn.get_processing_block_state(pb_id)
            pb_status = pb_state.get("status")
            assert pb_status == "RUNNING"

        deploy = work_phase.ee_deploy_dask(deploy_name, n_workers, calc, (1, 5))

        for txn in CONFIG_DB_CLIENT.txn():
            deploy_id = deploy.get_id()
            if deploy_id is not None:
                deployment_list = txn.list_deployments()
                assert deploy_id in deployment_list
                break
            txn.loop(wait=True)

        for txn in CONFIG_DB_CLIENT.txn():
            state = txn.get_processing_block_state(pb_id)
            deployments = state.get("deployments")
            deployments[deploy_id] = "FINISHED"
            state["deployments"] = deployments
            txn.update_processing_block_state(pb_id, state)

    for txn in CONFIG_DB_CLIENT.txn():
        pb_state = txn.get_processing_block_state(pb_id)
        pb_status = pb_state.get("status")
        assert pb_status == "FINISHED"


@patch.dict(os.environ, MOCK_ENV_VARS)
def test_receive_addresses():
    """Test generating and updating receive addresses."""

    # Wipe the config DB
    wipe_config_db()

    # Create sbi and pb
    create_sbi_pbi()

    # Create processing block states
    create_pb_states()

    pb_id = "pb-mvp01-20200425-00000"
    pb = workflow.ProcessingBlock(pb_id)
    work_phase = pb.create_phase("Work", [])

    # Get the expected receive addresses from the data file
    receive_addresses_expected = read_receive_addresses()

    with work_phase:
        for txn in CONFIG_DB_CLIENT.txn():
            sbi_list = txn.list_scheduling_blocks()
            for sbi_id in sbi_list:

                work_phase.ee_deploy_helm("test-receive")

                # Get the channel link map from SBI
                scan_types = pb.get_scan_types()
                pb.receive_addresses(scan_types)

                state = txn.get_processing_block_state(pb_id)
                pb_receive_addresses = state.get("receive_addresses")
                assert pb_receive_addresses == receive_addresses_expected
                validate(SDP_RECVADDRS + SCHEMA_VERSION, pb_receive_addresses, 2)

                # Testing with two channels
                pb.receive_addresses(SCAN_TYPES)
                pb_state = txn.get_processing_block_state(pb_id)
                recv_address = pb_state.get("receive_addresses")
                assert recv_address == RECV_ADDRESS
                validate(SDP_RECVADDRS + SCHEMA_VERSION, recv_address, 2)

                # Set scheduling block instance to FINISHED
                sbi = {"subarray_id": None, "status": "FINISHED"}
                sbi_state = txn.get_scheduling_block(sbi_id)
                sbi_state.update(sbi)
                txn.update_scheduling_block(sbi_id, sbi_state)

    for txn in CONFIG_DB_CLIENT.txn():
        pb_state = txn.get_processing_block_state(pb_id)
        pb_status = pb_state.get("status")
        assert pb_status == "FINISHED"


@patch.dict(os.environ, MOCK_ENV_VARS)
def test_dns_name():
    """Test generating dns name."""

    # Wipe the config DB
    wipe_config_db()

    # Create sbi and pb
    create_sbi_pbi()

    # Create processing block states
    create_pb_states()

    pb_id = "pb-mvp01-20200425-00000"
    pb = workflow.ProcessingBlock(pb_id)
    work_phase = pb.create_phase("Work", [])
    expected_dns_name = [
        "test-recv-0.receive.test-sdp.svc.cluster.local",
        "proc-pb-mvp01-20200425-00000-test-receive-0.receive.sdp.svc.cluster.local",
    ]

    with work_phase:
        for txn in CONFIG_DB_CLIENT.txn():
            sbi_list = txn.list_scheduling_blocks()

            for sbi_id in sbi_list:

                work_phase.ee_deploy_helm("test-receive")

                # Get scan types
                scan_types = pb.get_scan_types()

                # Testing with just passing scan types
                pb.receive_addresses(scan_types)
                state = txn.get_processing_block_state(pb_id)
                pb_recv_addresses = state.get("receive_addresses")
                pb_science_host = pb_recv_addresses["science_A"].get("host")
                assert pb_science_host[0][1] == expected_dns_name[1]

                # Testing with statefulset name, service name and namespace
                pb.receive_addresses(scan_types, "test-recv", "receive", "test-sdp")
                state = txn.get_processing_block_state(pb_id)
                pb_receive_addresses = state.get("receive_addresses")
                pb_cal_host = pb_receive_addresses["calibration_B"].get("host")
                assert pb_cal_host[0][1] == expected_dns_name[0]

                # Set scheduling block instance to FINISHED
                sbi_state = txn.get_scheduling_block(sbi_id)
                sbi_state.update({"subarray_id": None, "status": "FINISHED"})
                txn.update_scheduling_block(sbi_id, sbi_state)

    for txn in CONFIG_DB_CLIENT.txn():
        pb_state = txn.get_processing_block_state(pb_id)
        assert pb_state.get("status") == "FINISHED"


# -----------------------------------------------------------------------------
# Ancillary functions
# -----------------------------------------------------------------------------


def wipe_config_db():
    """Remove all entries in the config DB."""
    CONFIG_DB_CLIENT.backend.delete("/pb", must_exist=False, recursive=True)
    CONFIG_DB_CLIENT.backend.delete("/sb", must_exist=False, recursive=True)
    CONFIG_DB_CLIENT.backend.delete("/deploy", must_exist=False, recursive=True)


def create_work_phase(pb_id):
    """Create work phase."""
    pb = workflow.ProcessingBlock(pb_id)
    in_buffer_res = pb.request_buffer(100e6, tags=["sdm"])
    out_buffer_res = pb.request_buffer(10 * 6e15 / 3600, tags=["visibilities"])
    work_phase = pb.create_phase("Work", [in_buffer_res, out_buffer_res])
    return work_phase


def create_sbi_pbi():
    """Create scheduling block and processing block."""
    sbi, pbs = get_sbi_pbs()
    for txn in CONFIG_DB_CLIENT.txn():
        sbi_id = sbi.get("id")
        if sbi_id is not None:
            txn.create_scheduling_block(sbi_id, sbi)
        for pb in pbs:
            txn.create_processing_block(pb)


def get_sbi_pbs():
    """Get SBI and PBs from configuration string."""
    config = read_configuration_string()

    sbi_id = config.get("id")
    sbi = {
        "id": sbi_id,
        "subarray_id": SUBARRAY_ID,
        "scan_types": config.get("scan_types"),
        "pb_realtime": [],
        "pb_batch": [],
        "pb_receive_addresses": None,
        "current_scan_type": None,
        "scan_id": None,
        "status": "ACTIVE",
    }

    pbs = []
    for pbc in config.get("processing_blocks"):
        pb_id = pbc.get("id")
        wf_type = pbc.get("workflow").get("type")
        sbi["pb_" + wf_type].append(pb_id)
        if "dependencies" in pbc:
            dependencies = pbc.get("dependencies")
        else:
            dependencies = []
        pb = ska_sdp_config.ProcessingBlock(
            pb_id,
            sbi_id,
            pbc.get("workflow"),
            parameters=pbc.get("parameters"),
            dependencies=dependencies,
        )
        pbs.append(pb)

    return sbi, pbs


def create_pb_states():
    """Create PB states in the config DB.

    This creates the PB states with status = RUNNING, and for any workflow
    matching the list of receive workflows, it adds the receive addresses.

    """

    for txn in CONFIG_DB_CLIENT.txn():
        pb_list = txn.list_processing_blocks()
        for pb_id in pb_list:
            pb_state = txn.get_processing_block_state(pb_id)
            if pb_state is None:
                pb_state = {"status": "RUNNING"}
                txn.create_processing_block_state(pb_id, pb_state)


def read_configuration_string():
    """Read configuration string from JSON file."""
    return read_json_data("configuration_string.json", decode=True)


def read_receive_addresses():
    """Read receive addresses from JSON file."""
    return read_json_data("receive_addresses.json", decode=True)


def read_json_data(filename, decode=False):
    """Read JSON file from data directory.

    :param decode: decode the JSON dat into Python

    """
    path = os.path.join(os.path.dirname(__file__), "data", filename)
    with open(path, "r") as file:
        data = file.read()
    if decode:
        data = json.loads(data)
    return data

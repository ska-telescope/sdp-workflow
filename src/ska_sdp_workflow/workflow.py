"""High-level API for SKA SDP workflows."""
# pylint: disable=invalid-name
# pylint: disable=no-self-use

import logging
import os
import sys
import ska.logging
import ska_sdp_config

from ska_telmodel.sdp.version import SDP_RECVADDRS_PREFIX

from .phase import Phase
from .buffer_request import BufferRequest
from .feature_toggle import FeatureToggle


FEATURE_CONFIG_DB = FeatureToggle("config_db", True)
SCHEMA_VERSION = "0.2"

# Initialise logging
ska.logging.configure_logging()
LOG = logging.getLogger("ska_sdp_workflow")
LOG.setLevel(logging.DEBUG)


def new_config_db():
    """Return an SDP configuration client (factory function)."""
    backend = "etcd3" if FEATURE_CONFIG_DB.is_active() else "memory"
    LOG.info("Using config DB %s backend", backend)
    config_db = ska_sdp_config.Config(backend=backend)
    return config_db


class ProcessingBlock:
    """
    Claim the processing block.

    :param pb_id: processing block ID
    :type pb_id: str, optional
    """

    def __init__(self, pb_id=None):
        # Get connection to config DB
        LOG.info("Opening connection to config DB")
        self._config = new_config_db()

        # Processing block ID
        if pb_id is None:
            self._pb_id = sys.argv[1]
        else:
            self._pb_id = pb_id
        LOG.debug("Processing Block ID %s", self._pb_id)

        # Claim processing block
        for txn in self._config.txn():
            txn.take_processing_block(self._pb_id, self._config.client_lease)
            pb = txn.get_processing_block(self._pb_id)
        LOG.info("Claimed processing block")

        # Processing Block
        self._pb = pb

        # Scheduling Block Instance ID
        self._sbi_id = pb.sbi_id

        # DNS name
        self._service_name = "receive"
        self._chart_name = None
        self._namespace = None

    def receive_addresses(
        self, scan_types, chart_name=None, service_name=None, namespace=None
    ):
        """
        Generate receive addresses and update the processing block state.

        :param scan_types: Scan types
        :param chart_name: Name of the statefulset
        :param service_name: Name of the headless service
        :param namespace: namespace where its going to be deployed
        :type scan_types: list

        """
        # Generate receive addresses
        LOG.info("Generating receive addresses")
        receive_addresses = self._generate_receive_addresses(
            scan_types, chart_name, service_name, namespace
        )

        # Update receive addresses in processing block state
        LOG.info("Updating receive addresses in processing block state")
        for txn in self._config.txn():
            state = txn.get_processing_block_state(self._pb_id)
            state["receive_addresses"] = receive_addresses
            txn.update_processing_block_state(self._pb_id, state)

        # Write pb_id in pb_receive_addresses in SBI
        LOG.info("Writing PB ID to pb_receive_addresses in SBI")
        for txn in self._config.txn():
            sbi = txn.get_scheduling_block(self._sbi_id)
            sbi["pb_receive_addresses"] = self._pb_id
            txn.update_scheduling_block(self._sbi_id, sbi)

    def get_parameters(self, schema=None):
        """
        Get workflow parameters from processing block.

        The schema checking is not currently implemented.

        :param schema: schema to validate the parameters
        :returns: processing block parameters
        :rtype: dict

        """
        parameters = self._pb.parameters
        if schema is not None:
            LOG.info("Validating parameters against schema")

        return parameters

    def get_scan_types(self):
        """
        Get scan types from the scheduling block instance.

        This is only supported for real-time workflows

        :returns: scan types
        :rtype: list

        """
        LOG.info("Retrieving channel link map from SBI")
        for txn in self._config.txn():
            sbi = txn.get_scheduling_block(self._sbi_id)
            scan_types = sbi.get("scan_types")

        return scan_types

    def request_buffer(self, size, tags):
        """
        Request a buffer reservation.

        This returns a buffer reservation request that is used to create a
        workflow phase. These are currently only placeholders.

        :param size: size of the buffer
        :type size: float
        :param tags: tags describing the type of buffer required
        :type tags: list of str
        :returns: buffer reservation request
        :rtype: :class:`BufferRequest`

        """
        return BufferRequest(size, tags)

    def create_phase(self, name, requests):
        """
        Create a workflow phase for deploying execution engines.

        The phase is created with a list of resource requests which must be
        satisfied before the phase can start executing. For the time being the
        only resource requests are (placeholder) buffer reservations, but
        eventually this will include compute requests too.

        :param name: name of the phase
        :type name: str
        :param requests: resource requests
        :type requests: list of :class:`BufferRequest`
        :returns: the phase
        :rtype: :class:`Phase`

        """
        workflow = self._pb.workflow
        workflow_type = workflow["type"]
        return Phase(
            name, requests, self._config, self._pb_id, self._sbi_id, workflow_type
        )

    def exit(self):
        """Close connection to the configuration."""

        LOG.info("Closing connection to config DB")
        self._config.close()

    def nested_parameters(self, parameters):
        """Convert flattened dictionary to nested dictionary.

        :param parameters: parameters to be converted

        :return: nested parameters
        """

        result = {}
        for keys, values in parameters.items():
            self._split_rec(keys, values, result)
        return result

    # -------------------------------------
    # Private methods
    # -------------------------------------

    def _generate_receive_addresses(
        self, scan_types, chart_name=None, service_name=None, namespace=None
    ):
        """
        Generate receive addresses for all scan types.

        This function generates a minimal fake response.

        :param scan_types: scan types from SBI
        :param chart_name: Name of the statefulset
        :param service_name: Name of the headless service
        :param namespace: namespace where its going to be deployed
        :return: receive addresses

        """
        receive_addresses = {}

        if service_name is not None:
            self._service_name = service_name

        if namespace is not None:
            self._namespace = namespace
        else:
            self._namespace = os.environ["SDP_HELM_NAMESPACE"]

        for scan_type in scan_types:
            channels = scan_type.get("channels")
            host = []
            port = []
            for chan in channels:
                start = chan.get("start")
                dns_name = self._generate_dns_name(start, chart_name)
                host.append(dns_name)
                port.append([start, 9000, 1])
            receive_addresses[scan_type.get("id")] = dict(host=host, port=port)

        # Add schema interface
        receive_addresses["interface"] = SDP_RECVADDRS_PREFIX + SCHEMA_VERSION
        return receive_addresses

    def _generate_dns_name(self, chan_start, chart_name=None):
        """Generate DNS name for the receive processes.

        :param chan_start: start of the channel
        :param chart_name: Name of the statefulset
        :return: dns name

        """

        if chart_name is not None:
            self._chart_name = chart_name
        else:
            for txn in self._config.txn():
                for deploy_id in txn.list_deployments():
                    if self._pb_id in deploy_id:
                        if "-receive" in deploy_id:
                            self._chart_name = deploy_id

        dns_name = [
            chan_start,
            self._chart_name
            + "-{}.".format(0)
            + self._service_name
            + "."
            + self._namespace
            + ".svc.cluster.local",
        ]

        return dns_name

    def _split_rec(self, keys, values, out):
        """Splitting keys in dictionary using recursive approach.

        :param keys: keys from the dictionary
        :param values: values from the dictionary
        :param out: output result
        """
        keys, *rest = keys.split(".", 1)
        if rest:
            self._split_rec(rest[0], values, out.setdefault(keys, {}))
        else:
            out[keys] = values

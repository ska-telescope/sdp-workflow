"""High-level API for SKA SDP workflows."""
# pylint: disable=invalid-name
# pylint: disable=no-self-use
# pylint: disable=too-many-instance-attributes
# pylint: disable=too-many-locals

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

        # Ports
        self._ports = []

    def receive_addresses(
        self,
        chart_name=None,
        service_name=None,
        namespace=None,
        configured_host_port=None,
    ):
        """
        Generate receive addresses and update the processing block state.

        :param scan_types: Scan types
        :param chart_name: Name of the statefulset
        :param service_name: Name of the headless service
        :param namespace: namespace where its going to be deployed
        :param configured_host_port: constructed host and port

        """
        # Generate receive addresses
        LOG.info("Generating receive addresses")
        receive_addresses = self._update_receive_addresses(
            chart_name, service_name, namespace, configured_host_port
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

    def configure_recv_processes_ports(
        self, scan_types, max_channels_per_process, port_start, channels_per_port
    ):
        """Calculate how many receive process(es) and ports are required.

        :param scan_types: scan types from SBI
        :param max_channels_per_process: maximum number of channels per process
        :param port_start: starting port the receiver will be listening in
        :param channels_per_port: number of channels to be sent to each port
        :returns: configured host and port
        :rtype: dict

        """
        # Initial variables
        port_count = 0
        configured_host_port = {}

        # Number of receive process
        num_process = 0

        # Total process to deployed
        total_process = 0

        for scan_type in scan_types:
            # Initial variables
            hosts = []
            ports = []
            prev_count = 0
            process_per_channel = 0
            entry = True

            for chan in scan_type.get("channels"):
                start = chan.get("start")
                prev_count = prev_count + chan.get("count")
                for i in range(0, chan.get("count"), max_channels_per_process):
                    if entry:
                        prev_count = prev_count + chan.get("count")
                        num_process = 1
                        entry = False
                    else:
                        prev_count = prev_count + chan.get("count")
                        if prev_count >= max_channels_per_process:
                            process_per_channel += 1
                            num_process += process_per_channel
                        if i == 0:
                            prev_count = 0

                    hosts.append(self._construct_host(start, process_per_channel))
                    if channels_per_port > 1:
                        for j in range(0, channels_per_port):
                            ports.append([start, port_start + j, 1, port_count])
                            port_count += 1
                    else:
                        ports.append([start, port_start, 1])
                    port_count = 0
                    start = start + max_channels_per_process

            if num_process > total_process:
                total_process = num_process

            configured_host_port[scan_type.get("id")] = dict(host=hosts, port=ports)

        return configured_host_port, total_process

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

    def _construct_host(self, chan_start, num_process):
        """Construct a template of the host address.

        :param chan_start: start of the channel
        :param num_process: number of receive process
        :return: constructed host address

        """
        host = [chan_start, "-{}.".format(num_process)]
        return host

    def _update_receive_addresses(
        self,
        chart_name=None,
        service_name=None,
        namespace=None,
        configured_host_port=None,
    ):
        """
        Generate receive addresses for all scan types.

        :param chart_name: Name of the statefulset
        :param service_name: Name of the headless service
        :param namespace: namespace where its going to be deployed
        :param configured_host_port: configured host and port
        :return: receive addresses

        """
        receive_addresses = {}

        if service_name is not None:
            self._service_name = service_name

        if namespace is not None:
            self._namespace = namespace
        else:
            self._namespace = os.environ["SDP_HELM_NAMESPACE"]

        receive_addresses = self._generate_dns_name(configured_host_port, chart_name)

        # Add schema interface
        receive_addresses["interface"] = SDP_RECVADDRS_PREFIX + SCHEMA_VERSION
        return receive_addresses

    def _generate_dns_name(self, configured_host_port, chart_name=None):
        """Generate DNS name for the receive processes.

        :param chan_start: start of the channel
        :param chart_name: Name of the statefulset
        :param configured_host_port: constructed host and port
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

        for values in configured_host_port.values():
            for host in values["host"]:
                dns_name = (
                    self._chart_name
                    + host[1]
                    + self._service_name
                    + "."
                    + self._namespace
                    + ".svc.cluster.local"
                )
                host[1] = dns_name

        return configured_host_port

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

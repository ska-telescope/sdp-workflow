"""High-level API for SKA SDP workflow."""
# pylint: disable=invalid-name
# pylint: disable=no-self-use

import logging
import sys
import ska.logging
import ska_sdp_config

from .phase import Phase
from .buffer_request import BufferRequest
from .feature_toggle import FeatureToggle

FEATURE_CONFIG_DB = FeatureToggle('config_db', True)

# Initialise logging
ska.logging.configure_logging()
LOG = logging.getLogger('ska_sdp_workflow')
LOG.setLevel(logging.DEBUG)


def new_config_db():
    """Return a config DB object (factory function)."""
    backend = 'etcd3' if FEATURE_CONFIG_DB.is_active() else 'memory'
    LOG.info("Using config DB %s backend", backend)
    config_db = ska_sdp_config.Config(backend=backend)
    return config_db


class ProcessingBlock:
    """Connection to SKA SDP Workflow library."""

    def __init__(self, pb_id=None):
        """Connect to config db and claim processing block

        :param pb_id: processing block id
        """

        # Get connection to config DB
        LOG.info('Opening connection to config DB')
        self._config = new_config_db()
        # self._config = ska_sdp_config.Config()

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
        LOG.info('Claimed processing block')

        # Processing Block
        self._pb = pb

        # Scheduling Block Instance ID
        self._sbi_id = pb.sbi_id

    def receive_addresses(self, scan_types):
        """Generate receive addresses and update the processing block state.

        :param scan_types: Scan types
        """

        # Generate receive addresses
        LOG.info('Generating receive addresses')
        receive_addresses = self._generate_receive_addresses(scan_types)

        # Update receive addresses in processing block state
        LOG.info('Updating receive addresses in processing block state')
        for txn in self._config.txn():
            state = txn.get_processing_block_state(self._pb_id)
            state['receive_addresses'] = receive_addresses
            txn.update_processing_block_state(self._pb_id, state)

        # Write pb_id in pb_receive_addresses in SBI
        LOG.info('Writing PB ID to pb_receive_addresses in SBI')
        for txn in self._config.txn():
            sbi = txn.get_scheduling_block(self._sbi_id)
            sbi['pb_receive_addresses'] = self._pb_id
            txn.update_scheduling_block(self._sbi_id, sbi)

    def get_parameters(self, schema=None):
        """Get workflow parameters from processing block.

        :param schema: schema to validate

        :returns: pb_parameters
        """
        parameters = self._pb.parameters
        if schema is not None:
            LOG.info("Validate parameters against schema")

        return parameters

    def get_scan_types(self):
        """Get scan types from scheduling block.

        :returns: scan_types
        """

        LOG.info('Retrieving channel link map from SBI')
        for txn in self._config.txn():
            sbi = txn.get_scheduling_block(self._sbi_id)
            scan_types = sbi.get('scan_types')

        return scan_types

    def request_buffer(self, size, tags):
        """Create a :class:`BufferRequest` for input and output buffer.

        :param size: size of the buffer
        :param tags: type of the buffer

        :returns: handle to the BufferRequest class

        """

        return BufferRequest(size, tags)

    def create_phase(self, name, requests):
        """Create a :class:`Phase` for deploying and
        monitoring execution engines.

        :param name: name of the phase getting created
        :param requests: list of buffer requests

        :returns: handle to the Phase class

        """
        workflow = self._pb.workflow
        workflow_type = workflow['type']
        return Phase(name, requests, self._config,
                     self._pb_id, self._sbi_id, workflow_type)

    def exit(self):
        """Close connection to config DB."""

        LOG.info('Closing connection to config DB')
        self._config.close()

    # -------------------------------------
    # Private methods
    # -------------------------------------

    def _minimal_receive_addresses(self, channels):
        """
        Generate a minimal version of the receive addresses for a single scan type.

        :param channels: list of channels
        :returns: receive addresses

        """
        host = []
        port = []
        for i, chan in enumerate(channels):
            start = chan.get('start')
            host.append([start, '192.168.0.{}'.format(i + 1)])
            port.append([start, 9000, 1])
        receive_addresses = dict(host=host, port=port)
        return receive_addresses

    def _generate_receive_addresses(self, scan_types):
        """
        Generate receive addresses for all scan types.

        This function generates a minimal fake response.

        :param scan_types: scan types from SBI
        :return: receive addresses

        """
        receive_addresses = {}
        for scan_type in scan_types:
            channels = scan_type.get('channels')
            receive_addresses[scan_type.get('id')] = \
                self._minimal_receive_addresses(channels)
        return receive_addresses

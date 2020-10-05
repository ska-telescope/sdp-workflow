"""High-level API for SKA SDP workflow."""
# pylint: disable=invalid-name
# pylint: disable=too-few-public-methods

import sys
import time
import os
import logging
import sys
# import ska.logging
import ska_sdp_config
import distributed

# ska.logging.configure_logging()
LOG = logging.getLogger('worklow')
LOG.setLevel(logging.DEBUG)


class ProcessingBlock:

    def __init__(self):
        """Initialise.

        Claim processing block.

        """

        # Get connection to config DB
        LOG.info('Opening connection to config DB')
        self._config = ska_sdp_config.Config()

        # Processing block ID
        self._pb_id = sys.argv[1:]

        for txn in self._config.txn():
            txn.take_processing_block(self._pb_id, self._config.client_lease)
            pb = txn.get_processing_block(self._pb_id)
        LOG.info('Claimed processing block')

        # Processing Block
        self._pb = pb

        # Scheduling Block Instance ID
        self._sbi_id = pb.sbi_id

    def request_buffer(self, size, tags):

        return BufferRequest(size, tags)

    def request_compute(self, phases):

        return ComputeRequest(phases)

    def create_phase(self, name, reservation):

        return Phase(name, reservation)

    def resource_request(self):
        """Make resource request, assuming input is in the form of a dict.

        """

        # Set state to indicate workflow is waiting for resources.
        LOG.info('Setting status to WAITING')
        for txn in self._config.txn():
            state = txn.get_processing_block_state(self._pb_id)
            state['status'] = 'WAITING'
            txn.update_processing_block_state(self._pb_id, state)

        # Wait for resources_available to be true
        LOG.info('Waiting for resources to be available')
        for txn in self._config.txn():
            state = txn.get_processing_block_state(self._pb_id)
            ra = state.get('resources_available')
            if ra is not None and ra:
                LOG.info('Resources are available')
                break
            txn.loop(wait=True)

    def wait_loop(self):
        """Wait loop."""

        return self._config.txn()

    def is_finished(self, txn):
        sbi = txn.get_scheduling_block(self._sbi_id)
        status = sbi.get('status')
        if status in ['FINISHED']:
            LOG.info('SBI is %s', status)

            # Set state to indicate processing has ended
            LOG.info('Setting status to %s', status)
            state = txn.get_processing_block_state(self._pb_id)
            state['status'] = status
            txn.update_processing_block_state(self._pb_id, state)
            finished = True
        else:
            finished = False
        return finished

    def is_error(self, txn):
        sbi = txn.get_scheduling_block(self._sbi_id)
        status = sbi.get('status')
        if status in ['CANCELLED']:
            LOG.info('SBI is %s', status)
            error = True
        else:
            error = False
        return error

    def set_error(self, txn, message):
        state = txn.get_processing_block_state(self._pb_id)
        state['status'] = 'CANCELLED'
        txn.update_processing_block_state(self._pb_id, state)
        LOG.info(message)

    def receive_addresses(self, scan_types):
        """Generate receive addresses and update it in the processing block state.

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
        """Get workflow parameters from processing block as a dict, parsing with schema.

        :param pb_id: processing block ID
        :returns: pb_parameters

        """
        parameters = self._pb.parameters
        if schema is not None:
            LOG.info("Validate parameters aganist schema")
        return parameters

    def get_scan_types(self):
        """Get scan types."""

        LOG.info('Retrieving channel link map from SBI')
        for txn in self._config.txn():
            sbi = txn.get_scheduling_block(self._sbi_id)
            scan_types = sbi.get('scan_types')

        return scan_types

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
            receive_addresses[scan_type.get('id')] = self._minimal_receive_addresses(channels)
        return receive_addresses


class BufferRequest:
    def __init__(self, size, tags):
        """Init"""


class ComputeRequest:
    def __init__(self, phases):
        """Init"""


class Phase:
    def __init__(self, name, list_reservations):
        """Init"""

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


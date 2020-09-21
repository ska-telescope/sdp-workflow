"""High-level API for SKA SDP workflow."""
# pylint: disable=invalid-name
# pylint: disable=too-few-public-methods

import sys
import time
import logging
import ska_sdp_config

LOG = logging.getLogger('worklow')
LOG.setLevel(logging.DEBUG)


class Workflow:

    def __init__(self):
        """Initialise."""
        # Get connection to config DB
        LOG.info('Opening connection to config DB')
        self._config = ska_sdp_config.Config()

    def claim_processing_block(self, pb_id):
        """Claim processing block.

        :param pb_id: processing block ID
        :returns: Scheduling block ID

        """
        for txn in self._config.txn():
            txn.take_processing_block(pb_id, self._config.client_lease)
            pb = txn.get_processing_block(pb_id)
        LOG.info('Claimed processing block')

        sbi_id = pb.sbi_id
        return sbi_id

    def resource_request(self, pb_id):
        """Make resource request, assuming input is in the form of a dict.

        :param pb_id: processing block ID

        """
        # Set state to indicate workflow is waiting for resources.
        LOG.info('Setting status to WAITING')
        for txn in self._config.txn():
            state = txn.get_processing_block_state(pb_id)
            state['status'] = 'WAITING'
            txn.update_processing_block_state(pb_id, state)

        # Wait for resources_available to be true
        LOG.info('Waiting for resources to be available')
        for txn in self._config.txn():
            state = txn.get_processing_block_state(pb_id)
            ra = state.get('resources_available')
            if ra is not None and ra:
                LOG.info('Resources are available')
                break
            txn.loop(wait=True)

    # def release(self, status):
    #     pass

    def process_started(self, pb_id):
        """The process is started.

        :param pb_id: processing block ID

        """
        # Set state to indicate processing has started
        LOG.info('Setting status to RUNNING')
        for txn in self._config.txn():
            state = txn.get_processing_block_state(pb_id)
            state['status'] = 'RUNNING'
            txn.update_processing_block_state(pb_id, state)

    def monitor_sbi(self, sbi_id, pb_id):
        """For real-time workflows, wait for something to change in the SBI.

        :param pb_id: processing block ID
        :param sbi_id: scheduling block ID

        """
        # Wait until SBI is marked as FINISHED or CANCELLED
        LOG.info('Waiting for SBI to end')
        for txn in self._config.txn():
            sbi = txn.get_scheduling_block(sbi_id)
            status = sbi.get('status')
            if status in ['FINISHED', 'CANCELLED']:
                LOG.info('SBI is %s', status)
                break
            txn.loop(wait=True)

        # Set state to indicate processing has ended
        LOG.info('Setting status to %s', status)
        for txn in self._config.txn():
            state = txn.get_processing_block_state(pb_id)
            state['status'] = status
            txn.update_processing_block_state(pb_id, state)

    def monitor_sbi_batch(self, pb_id, duration):
        """Monitor SBI for batch workflows.

        :param pb_id: processing block ID
        :param duration: duration parameter

        """
        # Do some 'processing' for the required duration
        LOG.info('Starting processing for %f s', duration)
        time.sleep(duration)
        LOG.info('Finished processing')

        # Set state to indicate processing has ended
        LOG.info('Setting status to FINISHED')
        for txn in self._config.txn():
            state = txn.get_processing_block_state(pb_id)
            state['status'] = 'FINISHED'
            txn.update_processing_block_state(pb_id, state)

    def receive_addresses(self, scan_types, sbi_id, pb_id):
        """Generate receive addresses and update it in the processing block state.

        :param scan_types: Scan types
        :param sbi_id: scheduling block ID
        :param pb_id: processing block ID

        """

        # Generate receive addresses
        LOG.info('Generating receive addresses')
        receive_addresses = self._generate_receive_addresses(scan_types)

        # Update receive addresses in processing block state
        LOG.info('Updating receive addresses in processing block state')
        for txn in self._config.txn():
            state = txn.get_processing_block_state(pb_id)
            state['receive_addresses'] = receive_addresses
            txn.update_processing_block_state(pb_id, state)

        # Write pb_id in pb_receive_addresses in SBI
        LOG.info('Writing PB ID to pb_receive_addresses in SBI')
        for txn in self._config.txn():
            sbi = txn.get_scheduling_block(sbi_id)
            sbi['pb_receive_addresses'] = pb_id
            txn.update_scheduling_block(sbi_id, sbi)

    def get_parameters(self, pb_id):
        """Get workflow parameters from processing block as a dict, parsing with schema.

        :param pb_id: processing block ID
        :returns: duration parameter - TEMPORARY

        """
        for txn in self._config.txn():
            pb = txn.get_processing_block(pb_id)
            # TODO (NJT): Not just duration parameter - But this just for the time being
            # Get parameter and parse it
            duration = pb.parameters.get('duration')
            if duration is None:
                duration = 60.0
            LOG.info('duration: %f s', duration)
        return duration

    def get_scan_types(self, sbi_id):
        """Get scan types.

        :param sbi_id: Scheduling block ID

        """
        LOG.info('Retrieving channel link map from SBI')
        for txn in self._config.txn():
            sbi = txn.get_scheduling_block(sbi_id)
            scan_types = sbi.get('scan_types')

        return scan_types

    # def get_definition(self, pb_id):
    #     pass

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





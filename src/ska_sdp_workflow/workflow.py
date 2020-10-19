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


# ska.logging.configure_logging()
LOG = logging.getLogger('worklow')
LOG.setLevel(logging.DEBUG)


class ProcessingBlock:
    """SDP Workflow"""

    def __init__(self, pb_id=None):
        """Initialise.

        Claim processing block.

        :param pb_id: processing block id
        """

        # Get connection to config DB
        LOG.info('Opening connection to config DB')
        self._config = ska_sdp_config.Config()

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
            LOG.info("Validate parameters against schema")

        LOG.info(parameters)
        return parameters

    def get_scan_types(self):
        """Get scan types."""

        LOG.info('Retrieving channel link map from SBI')
        for txn in self._config.txn():
            sbi = txn.get_scheduling_block(self._sbi_id)
            scan_types = sbi.get('scan_types')

        return scan_types

    def request_buffer(self, size, tags):

        return BufferRequest(size, tags)

    def request_compute(self, phases):

        return ComputeRequest(phases)

    def create_phase(self, name, reservation):
        """Create either real-time or batch work phase."""
        workflow = self._pb.workflow
        if workflow['type'] == 'realtime':
            LOG.debug("Creating Real-time work phase")
            return RealTimePhase(name, reservation, self._config,
                                 self._pb_id, self._sbi_id)
        else:
            LOG.debug("Creating batch work phase")
            return BatchPhase()

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
        LOG.info("Buffer Requested")


class ComputeRequest:
    def __init__(self, phases):
        """Init"""


class BatchPhase:
    def __init__(self, name, list_reservations, config, pb_id):
        """Init"""
        self._name = name
        self._reservations = list_reservations
        self._config = config
        self._pb_id = pb_id
        self._deploy = None
        self._list_deployment = []

    def __enter__(self):
        '''Check if the pb is cancelled or sbi is finished and also if the resources are
        available'''

        # Create an event loop here

        # Set state to indicate workflow is waiting for resources
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

    def deploy(self, d_name=None, d_type=None, d_chart=None):
        """Deploy Execution Engine."""

        LOG.info("Inside the deploy method")
        if d_name is not None:
            deploy_id = 'proc-{}-{}}'.format(self._pb_id, d_name)
            LOG.info(deploy_id)
            self._deploy = ska_sdp_config.Deployment(deploy_id, d_type, d_chart)
            for txn in self._config.txn():
                txn.create_deployment(self._deploy)
                self._list_deployment.append(self._deploy)
        else:
            # Set state to indicate processing has started
            LOG.info('Setting status to RUNNING')
            for txn in self._config.txn():
                state = txn.get_processing_block_state(self._pb_id)
                state['status'] = 'RUNNING'
                txn.update_processing_block_state(self._pb_id, state)

    def wait_loop(self):
        """Wait loop.

        :returns: config transaction"""

        LOG.info("Wait loop method")
        for txn in self._config.txn():
            LOG.info("Checking processing block")
            if not txn.is_processing_block_owner(self._pb_id):
                LOG.info('Lost ownership of the processing block')
                break

            # Check if the pb state is set to finished
            pb_state = txn.get_processing_block_state(self._pb_id)
            if pb_state in ['FINISHED', 'CANCELLED']:
                break

            LOG.info(self._list_deployment)
            for dpl in self._list_deployment:
                LOG.info(dpl.id)
                deploy_lists = txn.list_deployments()
                deploy_id = dpl.id
                if not deploy_id in deploy_lists:
                    LOG.info("No deployment inside the current list")
                    break

            txn.loop(wait=True)

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Wait until SBI is marked as FINISHED or CANCELLED

        LOG.info("Inside exit method")
        self.wait_loop()

        # Close connection to config DB
        LOG.info('Closing connection to config DB')
        self._config.close()


class RealTimePhase:
    def __init__(self, name, list_reservations, config, pb_id, sbi_id):
        """Init"""
        self._name = name
        self._reservations = list_reservations
        self._config = config
        self._pb_id = pb_id
        self._sbi_id = sbi_id
        self._deploy_id = None

    def __enter__(self):
        '''Check if the pb is cancelled or sbi is finished and also if the resources are
        available'''

        # Create an event loop here

        # Check if the pb is cancelled or sbi is finished or cancelled
        for txn in self._config.txn():
            pb_state = txn.get_processing_block_state(self._pb_id)
            sbi = txn.get_scheduling_block(self._sbi_id)
            sbi_status = sbi.get('status')
            if pb_state or sbi_status in ['FINISHED', 'CANCELLED']:
                LOG.info('PB is %s', pb_state)
                LOG.info('SBI is %s', sbi_status)

        # Set state to indicate workflow is waiting for resources
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

    def ee_deploy(self, deploy_name, deploy_type, image):
        """Deploy Dask execution engine.

        :param deploy_name: processing block ID
        :param deploy_type: Deploy type
        :param image: Docker image to deploy

        """
        # Make deployment
        if deploy_name is not None:
            LOG.info("EE Deploy")
            self._deploy_id = 'proc-{}-{}}'.format(self._pb_id, deploy_name)
            LOG.info(self._deploy_id)
            deploy = ska_sdp_config.Deployment(self._deploy_id,
                                               deploy_type, image)
            for txn in self._config.txn():
                txn.create_deployment(deploy)
        else:
            # Set state to indicate processing has started
            LOG.info('Setting status to RUNNING')
            for txn in self._config.txn():
                state = txn.get_processing_block_state(self._pb_id)
                state['status'] = 'RUNNING'
                txn.update_processing_block_state(self._pb_id, state)

    def ee_remove(self, deploy_id):
        """Remove Dask EE deployment.

        :param deploy_id: deployment ID

        """
        for txn in self._config.txn():
            deploy = txn.get_deployment(deploy_id)
            txn.delete_deployment(deploy)

    def is_sbi_finished(self, txn):
        sbi = txn.get_scheduling_block(self._sbi_id)
        LOG.info("SBI ID %s", sbi)
        status = sbi.get('status')
        LOG.info(status)
        if status in ['FINISHED', 'CANCELLED']:
            LOG.info('SBI is %s', status)
            # break
            # Set state to indicate processing has ended
            LOG.info('Setting status to %s', status)
            state = txn.get_processing_block_state(self._pb_id)
            state['status'] = status
            txn.update_processing_block_state(self._pb_id, state)
            finished = True
        else:
            LOG.info("STATUS IS NOT FINISHED OR CANCELLED")
            finished = False

        return finished

    def wait_loop(self):
        """Wait loop.

        :returns: config transaction"""

        LOG.info("Wait loop method")
        for txn in self._config.txn():
            LOG.info("Checking processing block")
            if not txn.is_processing_block_owner(self._pb_id):
                LOG.info('Lost ownership of the processing block')
                break

            # Check if the pb state is set to finished
            pb_state = txn.get_processing_block_state(self._pb_id)
            if pb_state in ['FINISHED', 'CANCELLED']:
                break

            if self.is_sbi_finished(txn):
                break

            txn.loop(wait=True)

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Wait until SBI is marked as FINISHED or CANCELLED

        LOG.info("Inside exit method")
        self.wait_loop()

        # Clean up deployment.
        if self._deploy_id is not None:
            LOG.info("Deploy ID %s", self._deploy_id)
            self.ee_remove(self._deploy_id)

        # Close connection to config DB
        LOG.info('Closing connection to config DB')
        self._config.close()

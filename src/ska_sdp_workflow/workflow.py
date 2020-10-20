"""High-level API for SKA SDP workflow."""
# pylint: disable=invalid-name
# pylint: disable=too-few-public-methods

import logging
import sys
import threading
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

    def create_phase(self, name, reservation):
        """Create work phase."""
        workflow = self._pb.workflow
        workflow_type = workflow['type']
        return Phase(name, reservation, self._config,
                     self._pb_id, self._sbi_id, workflow_type)

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


class Phase:
    def __init__(self, name, list_reservations, config, pb_id,
                 sbi_id, workflow_type):
        """Init"""
        self._name = name
        self._reservations = list_reservations
        self._config = config
        self._pb_id = pb_id
        self._sbi_id = sbi_id
        self._workflow_type = workflow_type
        self._deploy_id = None
        self._status = None

    def __enter__(self):
        '''Enter'''

        # Create an event loop here
        if self._workflow_type == 'batch':
            # Start event loop
            self._event_loop = self._start_event_loop()
            LOG.info("Event loop started")

        # Check if the pb is cancelled or sbi is finished or cancelled
        for txn in self._config.txn():
            if self._workflow_type == 'realtime':
                pb_state = txn.get_processing_block_state(self._pb_id)
                pb_status = pb_state.get('status')
                sbi = txn.get_scheduling_block(self._sbi_id)
                sbi_status = sbi.get('status')
                if pb_status or sbi_status in ['FINISHED', 'CANCELLED']:
                    # raise exception
                    LOG.info('PB is %s', pb_status)
                    LOG.info('SBI is %s', sbi_status)
            else:
                pb_state = txn.get_processing_block_state(self._pb_id)
                pb_status = pb_state.get('status')
                if pb_status in ['FINISHED', 'CANCELLED']:
                    LOG.info('PB is %s', pb_state)
                    # raise exception

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

    def _start_event_loop(self):
        """Start event loop"""
        thread = threading.Thread(
            target=self._set_status, name='EventLoop', daemon=True
        )
        thread.start()

    def _set_status(self):
        """Set attributes based on configuration.

        if `loop` is `True`, it acts as an event loop to watch for changes to
        the configuration. If `loop` is `False` it makes a single pass.

        :param loop: watch for changes to configuration and loop

        """
        for txn in self._config.txn():
            LOG.info("Check for changes in the config db")
            state = txn.get_processing_block_state(self._pb_id)
            pb_status = state.get('status')
            if pb_status in ['FINISHED', 'CANCELLED']:
                # if cancelled raise exception
                LOG.info('Inside event loop PB_STATUS is %s', pb_status)
                break
            else:
                LOG.info("WAITING FOR PB STATUS TO CHANGE")

            if not txn.is_processing_block_owner(self._pb_id):
                LOG.info('Lost ownership of the processing block')
                # raise exception
                break

            txn.loop(wait=True)


    def ee_deploy(self, deploy_name=None, deploy_type=None, image=None):
        """Deploy Dask execution engine.

        :param deploy_name: processing block ID
        :param deploy_type: Deploy type
        :param image: Docker image to deploy

        """
        # Make deployment
        if deploy_name is not None:
            self._deploy_id = 'proc-{}-{}'.format(self._pb_id, deploy_name)
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

    def ee_remove(self, deploy_id=None):
        """Remove EE deployment.

        :param deploy_id: deployment ID

        """
        if deploy_id is not None:
            for txn in self._config.txn():
                deploy = txn.get_deployment(deploy_id)
                txn.delete_deployment(deploy)
                self._deploy_id = None
                self.update_pb_state()
        else:
            self.update_pb_state()

    def is_deploy_finished(self):
        for txn in self._config.txn():
            list_deployments = txn.list_deployments()
            if self._deploy_id is not None:
                if self._deploy_id not in list_deployments:
                    LOG.info("Deployment Finished")
            txn.loop(wait=True)


    def is_sbi_finished(self, txn):
        sbi = txn.get_scheduling_block(self._sbi_id)
        status = sbi.get('status')
        if status in ['FINISHED', 'CANCELLED']:
            LOG.info('SBI is %s', status)
            self._status = status
            # if cancelled raise exception
            finished = True

        else:
            finished = False

        return finished

    def update_pb_state(self, status=None):
        """Update Processing Block State."""

        for txn in self._config.txn():
            # Set state to indicate processing has ended
            state = txn.get_processing_block_state(self._pb_id)
            if status is None:
                LOG.info('Setting status to FINISHED')
                state['status'] = 'FINISHED'
            else:
                LOG.info('Setting status to %s', self._status)
                state['status'] = self._status
            txn.update_processing_block_state(self._pb_id, state)

    def wait_loop(self):
        """Wait loop.

        :returns: config transaction"""

        for txn in self._config.txn():

            # Check if the pb state is set to finished
            pb_state = txn.get_processing_block_state(self._pb_id)
            pb_status = pb_state.get('status')
            if pb_status in ['FINISHED', 'CANCELLED']:
                LOG.info("Processing Block Finished")
                # if cancelled raise exception
                break

            if not txn.is_processing_block_owner(self._pb_id):
                LOG.info('Lost ownership of the processing block')
                # raise exception
                break

            if self.is_sbi_finished(txn):
                break

            txn.loop(wait=True)

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit"""

        # Wait until SBI is marked as FINISHED or CANCELLED
        if self._workflow_type == 'realtime':
            self.wait_loop()

            # Update Processing Block
            self.update_pb_state(self._status)
        else:
            self.is_deploy_finished()

        # Clean up deployment.
        if self._deploy_id is not None:
            self.ee_remove(self._deploy_id)

        # Close connection to config DB
        LOG.info('Closing connection to config DB')
        self._config.close()

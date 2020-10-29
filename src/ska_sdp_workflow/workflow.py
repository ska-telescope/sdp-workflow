"""High-level API for SKA SDP workflow."""
# pylint: disable=invalid-name
# pylint: disable=too-few-public-methods

import logging
import sys
import threading
# import ska.logging
import ska_sdp_config
import queue
import os
import distributed

# ska.logging.configure_logging()
LOG = logging.getLogger('workflow')
LOG.setLevel(logging.DEBUG)


class ProcessingBlock:
    """Connection to SKA SDP Workflow library."""

    def __init__(self, pb_id=None):
        """Connect to config db and claim processing block

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
        """Generate receive addresses and update it
        in the processing block state.

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

    def create_phase(self, name, reservation):
        """Create a :class:`Phase` for deploying and
        monitoring execution engines.

        :param name: name of the phase getting created
        :param reservation: list of buffer reservations

        :returns: handle to the Phase class

        """
        workflow = self._pb.workflow
        workflow_type = workflow['type']
        return Phase(name, reservation, self._config,
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
            receive_addresses[scan_type.get('id')] = self._minimal_receive_addresses(channels)
        return receive_addresses


# -------------------------------------
# BufferRequest Class
# -------------------------------------


class BufferRequest:
    """Buffer request class. Currently just a placeholder."""

    def __init__(self, size, tags):
        """Initialise"""
        LOG.info("Buffer Requested")


# -------------------------------------
# Phase Class
# -------------------------------------

class Phase:
    """Connection to the Phase class. """

    def __init__(self, name, list_reservations, config, pb_id,
                 sbi_id, workflow_type):
        """Initialise"""
        self._name = name
        self._reservations = list_reservations
        self._config = config
        self._pb_id = pb_id
        self._sbi_id = sbi_id
        self._workflow_type = workflow_type
        self._deploy_id_list = []
        self._status = None
        self._q = None

    def __enter__(self):
        """Before entering, checks if the pb is cancelled or finished.
        For real-time workflows checks if sbi is cancelled or finished.

        Waits for resources to be available and then creates an event loop
        for the batch workflow.
        """

        for txn in self._config.txn():
            self.check_state(txn)

        # Set state to indicate workflow is waiting for resources
        LOG.info('Setting status to WAITING')
        for txn in self._config.txn():
            self.check_state(txn)
            state = txn.get_processing_block_state(self._pb_id)
            state['status'] = 'WAITING'
            txn.update_processing_block_state(self._pb_id, state)

        # Wait for resources_available to be true
        LOG.info('Waiting for resources to be available')
        for txn in self._config.txn():
            self.check_state(txn)
            state = txn.get_processing_block_state(self._pb_id)
            ra = state.get('resources_available')
            if ra is not None and ra:
                LOG.info('Resources are available')
                break
            txn.loop(wait=True)

        # Create an event loop here
        if self._workflow_type == 'batch':
            # Start event loop
            self._event_loop = self._start_event_loop()
            LOG.info("Event loop started")

        # Set state to indicate processing has started
        LOG.info('Setting status to RUNNING')
        for txn in self._config.txn():
            self.check_state(txn)
            state = txn.get_processing_block_state(self._pb_id)
            state['status'] = 'RUNNING'
            txn.update_processing_block_state(self._pb_id, state)

            # # spawn a pool of thread, and pass them queue instance
            # self._q = queue.Queue()
            # t = ProcessingThread(self._q)
            # t.setDaemon(True)
            # t.start()

    def check_state(self, txn):
        """Check if the pb is cancelled or sbi is finished or cancelled"""
        LOG.info("Checking PB and SBI states")
        pb_state = txn.get_processing_block_state(self._pb_id)
        pb_status = pb_state.get('status')
        if pb_status in ['FINISHED', 'CANCELLED']:
            raise Exception('PB is {}'.format(pb_state))

        if self._workflow_type == 'realtime':
            sbi = txn.get_scheduling_block(self._sbi_id)
            sbi_status = sbi.get('status')
            if sbi_status in ['FINISHED', 'CANCELLED']:
                raise Exception('PB is {}'.format(sbi_status))

    def ee_deploy_dask(self, execute_func, args):
        """."""

        return Deployment(self._pb_id, self._config, execute_func, args)
        # workflow = self._pb.workflow
        # workflow_type = workflow['type']
        # return Phase(name, reservation, self._config,
        #              self._pb_id, self._sbi_id, workflow_type)

    def is_deploy_finished(self):
        """Waits until all the deployments are finished and removed
         from the list."""
        for txn in self._config.txn():
            list_deployments = txn.list_deployments()
            for deploy_id in self._deploy_id_list:
                if deploy_id not in list_deployments:
                    self._deploy_id_list.remove(deploy_id)
            else:
                LOG.info("Deployment Finished")
                break

            txn.loop(wait=True)

    def is_sbi_finished(self, txn):
        """Checks if the sbi are finished or cancelled."""
        sbi = txn.get_scheduling_block(self._sbi_id)
        status = sbi.get('status')
        if status in ['FINISHED', 'CANCELLED']:
            if status is 'CANCELLED':
                raise Exception('SBI is {}'.format(status))
            LOG.info('SBI is %s', status)
            self._status = status
            finished = True

        else:
            finished = False

        return finished

    def update_pb_state(self, status=None):
        """Update Processing Block State.

        :param status: Default status is set to finished unless provided
        """
        if status is not None:
            self._status = status

        for txn in self._config.txn():
            # Set state to indicate processing has ended
            state = txn.get_processing_block_state(self._pb_id)
            if self._status is None:
                LOG.info('Setting PB status to FINISHED')
                state['status'] = 'FINISHED'
            else:
                LOG.info('Setting PB status to %s', self._status)
                state['status'] = self._status
            txn.update_processing_block_state(self._pb_id, state)

    def wait_loop(self):
        """Wait loop to check if pb abd sbi is set to finished or cancelled
        and also to check if pb lost ownership.
        """

        for txn in self._config.txn():

            # Check if the pb state is set to finished
            pb_state = txn.get_processing_block_state(self._pb_id)
            pb_status = pb_state.get('status')
            if pb_status in ['FINISHED', 'CANCELLED']:
                LOG.info("Processing Block is %s", pb_status)
                if pb_status is 'CANCELLED':
                    raise Exception('PB is {}'.format(pb_status))
                break

            if not txn.is_processing_block_owner(self._pb_id):
                raise Exception("Lost ownership of the processing block")

            if self.is_sbi_finished(txn):
                break

            txn.loop(wait=True)

    # def process_task(self, processes):
    #     """Spawn a thread and pass the processes to queue."""
    #
    #     for process in processes:
    #         self._q.put(process)
    #
    #     while not self._q.empty():
    #         pass
    #
    #     LOG.info("Queue is empty")
    #     self._q.join()
    #     LOG.info("Processing Done")
    #     self.update_pb_state()

    def _start_event_loop(self):
        """Start event loop"""
        thread = threading.Thread(
            target=self._check_status, name='EventLoop', daemon=True
        )
        thread.start()

    def _check_status(self):
        """Watch for changes in the config db"""

        for txn in self._config.txn():
            state = txn.get_processing_block_state(self._pb_id)
            pb_status = state.get('status')
            LOG.info("PB Status %s", pb_status)
            if pb_status in ['FINISHED', 'CANCELLED']:
                if pb_status is 'CANCELLED':
                    self._q.queue.clear()
                    raise Exception('PB is {}'.format(pb_status))
                break
            else:
                LOG.info("Checking Config db for changes...")

            if not txn.is_processing_block_owner(self._pb_id):
                LOG.error("Lost ownership of the processing block")
                self._q.queue.clear()
                # raise Exception("Lost ownership of the processing block")

            txn.loop(wait=True)

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Before exiting the phase class, checks if the sbi is marked as
        finished or cancelled for real-time workflows and updates processing
        block state.

        For batch-workflow waits until all the deployments are finished and
        processing block is set to finished."""

        if self._workflow_type == 'realtime':
            # Wait until SBI is marked as FINISHED or CANCELLED
            self.wait_loop()

            # Clean up deployment.
            LOG.info("Clean up deployment")
            if self._deploy_id_list:
                for deploy_id in self._deploy_id_list:
                    self.ee_remove(deploy_id)
                if self.is_deploy_finished():
                    self.update_pb_state()
            else:
                self.update_pb_state()
        else:
            # if self.is_deploy_finished():
            self.update_pb_state()

# -------------------------------------
# Deployment Class
# -------------------------------------

class Deployment:
    def __init__(self, pb_id, config, execute_func, args):
        self._pb_id = pb_id
        self._config = config

        x = threading.Thread(target=self._deploy_dask, args=(execute_func, args))
        x.setDaemon(True)
        x.start()
        self._x = x

    def _deploy_dask(self, func, args):
        # Deploy Dask with 2 workers.
        # This is done by adding the request to the configuration database,
        # where it will be picked up and executed by appropriate
        # controllers. In the full system this will involve external checks
        # for whether the workflow actually has been assigned enough resources
        # to do this - and for obtaining such assignments the workflow would
        # need to communicate with a scheduler process. But we are ignoring
        # all of that at the moment.
        LOG.info("Deploying Dask...")
        deploy_id = 'proc-{}-dask'.format(self._pb_id)
        deploy = ska_sdp_config.Deployment(
            deploy_id, "helm", {
                'chart': 'dask/dask',
                'values': {
                    'jupyter.enabled': 'false',
                    'jupyter.rbac': 'false',
                    'worker.replicas': 2,
                    # We want to access Dask in-cluster using a DNS name
                    'scheduler.serviceType': 'ClusterIP'
                }})
        for txn in self._config.txn():
            txn.create_deployment(deploy)

        # Wait for Dask to become available. At some point there will be a
        # way to learn about availability from the configuration database
        # (clearly populated by controllers querying Helm/Kubernetes).  So
        # for the moment we'll simply query the DNS name where we know
        # that Dask must become available eventually
        LOG.info("Waiting for Dask...")
        client = None

        for _ in range(200):
            try:
                client = distributed.Client(
                    deploy_id + '-scheduler.' +
                    os.environ['SDP_HELM_NAMESPACE'] + ':8786')
            except Exception as e:
                print(e)
        if client is None:
            LOG.error("Could not connect to Dask!")
            sys.exit(1)
        LOG.info("Connected to Dask")

        # Doing some silly calculation
        func(*args)

    def is_finished(self):
        if self._x.is_alive():
            print("Waiting...")
            return False
        else:
            return True

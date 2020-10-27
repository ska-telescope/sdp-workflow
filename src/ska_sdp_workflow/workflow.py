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
LOG = logging.getLogger('worklow')
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

    def __enter__(self):
        """Before entering, checks if the pb is cancelled or finished.
        For real-time workflows checks if sbi is cancelled or finished.

        Waits for resources to be available and then creates an event loop
        for the batch workflow.
        """

        # Check if the pb is cancelled or sbi is finished or cancelled
        for txn in self._config.txn():
            pb_state = txn.get_processing_block_state(self._pb_id)
            pb_status = pb_state.get('status')
            if pb_status in ['FINISHED', 'CANCELLED']:
                LOG.error('PB is %s', pb_state)
                raise Exception('PB is {}'.format(pb_state))

            if self._workflow_type == 'realtime':
                sbi = txn.get_scheduling_block(self._sbi_id)
                sbi_status = sbi.get('status')
                if sbi_status in ['FINISHED', 'CANCELLED']:
                    LOG.error('PB is %s', sbi_status)
                    raise Exception('PB is {}'.format(sbi_status))

        # Set state to indicate workflow is waiting for resources
        LOG.info('Setting status to WAITING')
        for txn in self._config.txn():
            state = txn.get_processing_block_state(self._pb_id)
            state['status'] = 'WAITING'
            raise Exception('PB is {}'.format(state['status']))
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

        # Create an event loop here
        if self._workflow_type == 'batch':
            # Start event loop
            self._event_loop = self._start_event_loop()
            LOG.info("Event loop started")

    def ee_deploy(self, deploy_name=None, deploy_type=None, chart=None,
                  image=None, n_workers=1, buffers=[]):
        """Deploy execution engine.

        :param deploy_name: processing block ID
        :param image: Docker image to deploy
        :param n_workers: number of Dask workers
        :param buffers: list of buffers to mount on Dask workers
        :return: deployment ID and Dask client handle

        """
        # Set state to indicate processing has started
        LOG.info('Setting status to RUNNING')
        for txn in self._config.txn():
            state = txn.get_processing_block_state(self._pb_id)
            state['status'] = 'RUNNING'
            txn.update_processing_block_state(self._pb_id, state)

        # Make deployment
        if deploy_name is not None:
            deploy_id = 'proc-{}-{}'.format(self._pb_id, deploy_name)
            if image is not None:
                values = {'image': image, 'worker.replicas': n_workers}
                for i, b in enumerate(buffers):
                    values['buffers[{}]'.format(i)] = b
                deploy = ska_sdp_config.Deployment(
                    deploy_id, deploy_type, {'chart': 'dask', 'values': values}
                )
            else:
                LOG.info(deploy_id)
                deploy = ska_sdp_config.Deployment(deploy_id,
                                                   deploy_type, chart)
            LOG.info(deploy)
            for txn in self._config.txn():
                txn.create_deployment(deploy)

            self._deploy_id_list.append(deploy_id)

            LOG.info("Waiting for Scheduler to become available...")
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
                # sys.exit(1)
                # raise exception
                return client, deploy_id
            LOG.info("Connected to Dask")

            return client, deploy_id

        return None

    def ee_remove(self, deploy_id=None):
        """Remove EE deployment.

        :param deploy_id: deployment ID

        """
        if deploy_id is not None:
            for txn in self._config.txn():
                deploy = txn.get_deployment(deploy_id)
                txn.delete_deployment(deploy)

    def is_deploy_finished(self):
        """Waits until all the deployments are finished and removed
         from the list."""
        for txn in self._config.txn():
            list_deployments = txn.list_deployments()
            if self._deploy_id_list:
                for deploy_id in self._deploy_id_list:
                    if deploy_id not in list_deployments:
                        LOG.info("Deployment Finished")
                        self._deploy_id_list.remove(deploy_id)
            else:
                LOG.info("Deployments all removed")
                break

            txn.loop(wait=True)

    def is_sbi_finished(self, txn):
        """Checks if the sbi are finished or cancelled."""
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
        """Update Processing Block State.

        :param status: Default status is set to finished unless provided
        """

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
        """Wait loop to check if pb abd sbi is set to finished or cancelled
        and also to check if pb lost ownership.
        """

        for txn in self._config.txn():

            # Check if the pb state is set to finished
            pb_state = txn.get_processing_block_state(self._pb_id)
            pb_status = pb_state.get('status')
            if pb_status in ['FINISHED', 'CANCELLED']:
                LOG.info("Processing Block is %s", pb_status)
                # if cancelled raise exception
                break

            if not txn.is_processing_block_owner(self._pb_id):
                LOG.info('Lost ownership of the processing block')
                # raise exception
                break

            if self.is_sbi_finished(txn):
                break

            txn.loop(wait=True)

    def process_task(self, processes):
        """Spawn a thread and pass the processes to queue."""
        # TODO - Need to do a thorough check-up

        # spawn a pool of thread, and pass them queue instance
        q = queue.Queue()

        t = ProcessingThread(q)
        t.setDaemon(True)
        t.start()

        for process in processes:
            q.put(process)

        while not q.empty():
            pass

        LOG.info("Queue is empty now")
        q.join()
        LOG.info("Processing Done")
        self.update_pb_state()

    def _start_event_loop(self):
        """Start event loop"""
        thread = threading.Thread(
            target=self._set_status, name='EventLoop', daemon=True
        )
        thread.start()

    def _set_status(self):
        """Watch for changes in the config db"""

        for txn in self._config.txn():

            state = txn.get_processing_block_state(self._pb_id)
            pb_status = state.get('status')
            if pb_status in ['FINISHED', 'CANCELLED']:
                # if cancelled raise exception
                break

            if not txn.is_processing_block_owner(self._pb_id):
                LOG.info('Lost ownership of the processing block')
                # raise exception
                break

            txn.loop(wait=True)

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Before exiting the phase class, checks if the sbi is marked as
        finished or cancelled for real-time workflows and updates processing
        block state.

        For batch-workflow waits until all the deployments are finished and
        processing block is set to finished."""

        LOG.info("Going to exit now")
        # Wait until SBI is marked as FINISHED or CANCELLED
        if self._workflow_type == 'realtime':
            self.wait_loop()

            # Clean up deployment.
            LOG.info("Clean up deployment")
            if self._deploy_id_list:
                for deploy_id in self._deploy_id_list:
                    self.ee_remove(deploy_id)

                if self.is_deploy_finished():
                    # Update Processing Block
                    self.update_pb_state(self._status)

            else:
                # Update Processing Block
                self.update_pb_state(self._status)

        else:
            if self.is_deploy_finished():
                # Update Processing Block
                self.update_pb_state(self._status)

# -------------------------------------
# Processing Thread Class
# -------------------------------------


class ProcessingThread(threading.Thread):
    """Add process to the queue and execute sequentially."""

    def __init__(self, queue):
        threading.Thread.__init__(self)
        self.queue = queue

    def run(self):
        while True:
            task = self.queue.get()
            func = task[0]
            args = task[1:]
            func(*args)
            self.queue.task_done()

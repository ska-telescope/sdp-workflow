"""High-level API for SKA SDP workflow."""
# pylint: disable=invalid-name
# pylint: disable=too-few-public-methods

import logging
import sys
import threading
# import ska.logging
import ska_sdp_config
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
            receive_addresses[scan_type.get('id')] = \
                self._minimal_receive_addresses(channels)
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
        self._deploy = None
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

        # Set state to indicate processing has started
        LOG.info('Setting status to RUNNING')
        for txn in self._config.txn():
            self.check_state(txn)
            state = txn.get_processing_block_state(self._pb_id)
            state['status'] = 'RUNNING'
            txn.update_processing_block_state(self._pb_id, state)

    def check_state(self, txn):
        """Check if the pb is cancelled or sbi is finished or cancelled"""
        LOG.info("Checking PB state")
        pb_state = txn.get_processing_block_state(self._pb_id)
        pb_status = pb_state.get('status')
        if pb_status in ['FINISHED', 'CANCELLED']:
            raise Exception('PB is {}'.format(pb_state))

        if self._workflow_type == 'realtime':
            LOG.info("Checking SBI state")
            sbi = txn.get_scheduling_block(self._sbi_id)
            sbi_status = sbi.get('status')
            if sbi_status in ['FINISHED', 'CANCELLED']:
                raise Exception('PB is {}'.format(sbi_status))

    def ee_deploy(self, deploy_name=None, func=None, f_args=None):
        if deploy_name is not None:
            self._deploy = HelmDeploy(self._pb_id, self._config, deploy_name)
            deploy_id = self._deploy.get_id()
            self._deploy_id_list.append(deploy_id)
        else:
            return HelmDeploy(self._pb_id, self._config, func=func, f_args=f_args)

    def ee_deploy_dask(self, name, func, f_args):
        """Deploy Dask and return a handle."""
        return DaskDeploy(self._pb_id, self._config, name, func, f_args)

    def is_sbi_finished(self, txn):
        """Checks if the sbi are finished or cancelled."""
        sbi = txn.get_scheduling_block(self._sbi_id)
        status = sbi.get('status')
        if status in ['FINISHED', 'CANCELLED']:
            if status is 'CANCELLED':
                raise Exception('SBI is {}'.format(status))
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
                    self._deploy.remove(deploy_id)
                self.update_pb_state()
            else:
                # This is for test_realtime workflow
                self.update_pb_state()
        else:
            self.update_pb_state()

# -------------------------------------
# Helm Deploy Class
# -------------------------------------


class HelmDeploy:
    def __init__(self, pb_id, config, deploy_name=None,
                 func=None, f_args=None,):
        self._pb_id = pb_id
        self._config = config
        self._deploy_id = None

        if deploy_name is not None:
            self.deploy(deploy_name)
        else:
            self.deploy(func=func, f_args=f_args)

    def deploy(self, deploy_name=None, func=None, f_args=None):
        """Deploy Execution Engine."""

        if deploy_name is not None:
            LOG.info("Deploying {} Workflow...".format(deploy_name))
            chart = {
                'chart': deploy_name,  # Helm chart deploy from the repo
            }
            self._deploy_id = 'proc-{}-{}'.format(self._pb_id, deploy_name)
            deploy = ska_sdp_config.Deployment(self._deploy_id,
                                               "helm", chart)
            for txn in self._config.txn():
                txn.create_deployment(deploy)
        else:
            LOG.info("Pretending to Create deployment.")
            # Running function to do some processing
            func(*f_args)
            LOG.info("Processing Done")

    def remove(self, deploy_id=None):
        """Remove Execution Engine."""
        if deploy_id is not None:
            self._deploy_id = deploy_id
        for txn in self._config.txn():
            deploy = txn.get_deployment(self._deploy_id)
            txn.delete_deployment(deploy)

# -------------------------------------
# Dask Deploy Class
# -------------------------------------


class DaskDeploy:
    def __init__(self, pb_id, config, deploy_name=None,
                 func=None, f_args=None,):
        self._pb_id = pb_id
        self._config = config
        self._deploy_id = None
        self._deploy_flag = False

        x = threading.Thread(target=self.deploy_dask,
                             args=(deploy_name, func, f_args))
        x.setDaemon(True)
        x.start()

    def deploy_dask(self, deploy_name, func, f_args):
        """Deploy Dask."""
        # Deploy Dask with 2 workers.
        # This is done by adding the request to the configuration database,
        # where it will be picked up and executed by appropriate
        # controllers. In the full system this will involve external checks
        # for whether the workflow actually has been assigned enough resources
        # to do this - and for obtaining such assignments the workflow would
        # need to communicate with a scheduler process. But we are ignoring
        # all of that at the moment.
        LOG.info("Deploying Dask...")
        self._deploy_id = 'proc-{}-{}'.format(self._pb_id, deploy_name)
        deploy = ska_sdp_config.Deployment(
            self._deploy_id, "helm", {
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
                    self._deploy_id + '-scheduler.' +
                    os.environ['SDP_HELM_NAMESPACE'] + ':8786')
            except Exception as e:
                print(e)
        if client is None:
            LOG.error("Could not connect to Dask!")
            sys.exit(1)
        LOG.info("Connected to Dask")

        # Doing some silly calculation
        result = func(*f_args)
        client.compute(result)

        self._deploy_flag = True

    def get_id(self):
        """Get deployment id"""
        return self._deploy_id

    def remove(self):
        """Remove Execution Engine."""
        for txn in self._config.txn():
            deploy = txn.get_deployment(self._deploy_id)
            txn.delete_deployment(deploy)

    def is_finished(self):
        """Checking if the deployment is finished."""
        for txn in self._config.txn():
            LOG.info("Checking PB state.")
            state = txn.get_processing_block_state(self._pb_id)
            pb_status = state.get('status')
            if pb_status in ['FINISHED', 'CANCELLED']:
                if pb_status is 'CANCELLED':
                    self.remove()
                    raise Exception('PB is {}'.format(pb_status))

            if not txn.is_processing_block_owner(self._pb_id):
                self.remove()
                raise Exception("Lost ownership of the processing block")

            if self._deploy_flag:
                self.remove()
                return True

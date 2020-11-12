"""Phase class module for SDP workflow."""
# pylint: disable=too-many-instance-attributes
# pylint: disable=too-many-arguments
# pylint: disable=inconsistent-return-statements

import logging

from .dask_deploy import DaskDeploy
from .helm_deploy import HelmDeploy
from .test_deploy import TestDeploy

LOG = logging.getLogger('ska_sdp_workflow')


class Phase:
    """Phase class.Connection to the declare phases. """

    def __init__(self, name, list_reservations, config, pb_id,
                 sbi_id, workflow_type):
        """Initialise

        :param name: name of the phase
        :param list_reservations: list of reservations
        :param config: config DB
        :param pb_id: processing block ID
        :param sbi_id: scheduling block instance ID
        :param workflow_type: workflow type

        """
        self._name = name
        self._reservations = list_reservations
        self._config = config
        self._pb_id = pb_id
        self._sbi_id = sbi_id
        self._workflow_type = workflow_type
        self._deploy_id_list = []
        self._deploy = None
        self._status = None
        self._deployment_status = None

    def __enter__(self):
        """Before entering, checks if the pb is cancelled or finished.
        For real-time workflows checks if sbi is cancelled or finished.

        Waits for resources to be available.
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
            r_a = state.get('resources_available')
            if r_a is not None and r_a:
                LOG.info('Resources are available')
                break
            txn.loop(wait=True)

        # Add deployments key to processing block state
        for txn in self._config.txn():
            self.check_state(txn)
            state = txn.get_processing_block_state(self._pb_id)
            if 'deployments' not in state:
                state['deployments'] = {}
                txn.update_processing_block_state(self._pb_id, state)

        # Set state to indicate processing has started
        LOG.info('Setting status to RUNNING')
        for txn in self._config.txn():
            self.check_state(txn)
            state = txn.get_processing_block_state(self._pb_id)
            state['status'] = 'RUNNING'
            txn.update_processing_block_state(self._pb_id, state)

    def check_state(self, txn):
        """Check if the pb is finished or cancelled. For real-time workflows
        check if sbi is finished or cancelled.

        :param txn: config db transaction

        """
        LOG.info("Checking PB state")
        pb_state = txn.get_processing_block_state(self._pb_id)
        pb_status = pb_state.get('status')
        if pb_status in ['FINISHED', 'CANCELLED']:
            raise Exception('PB is {}'.format(pb_state))

        if not txn.is_processing_block_owner(self._pb_id):
            raise Exception("Lost ownership of the processing block")

        if self._workflow_type == 'realtime':
            LOG.info("Checking SBI state")
            sbi = txn.get_scheduling_block(self._sbi_id)
            sbi_status = sbi.get('status')
            if sbi_status in ['FINISHED', 'CANCELLED']:
                raise Exception('PB is {}'.format(sbi_status))

    def ee_deploy_test(self, deploy_name, func=None, f_args=None):
        """Deploy a fake Execution Engine and returns a handle.

        :param deploy_name: deploy name
        :param func: function to process
        :param f_args: function arguments

        """
        return TestDeploy(self._pb_id, self._config, deploy_name,
                          func=func, f_args=f_args)

    def ee_deploy(self, deploy_name):
        """Deploy Helm Execution Engine.

        :param deploy_name: deploy name

        """
        self._deploy = HelmDeploy(self._pb_id, self._config, deploy_name)
        deploy_id = self._deploy.get_id()
        self._deploy_id_list.append(deploy_id)

    def ee_deploy_dask(self, name, n_workers, func, f_args):
        """Deploy Dask Execution Engine and returns a handle.

        :param name: deploy name
        :param n_workers: number of dask workers
        :param func: function to process
        :param f_args: function arguments

        """
        return DaskDeploy(self._pb_id, self._config, name,
                          n_workers, func, f_args)

    def ee_remove(self):
        """Remove Execution Engine."""
        for txn in self._config.txn():
            state = txn.get_processing_block_state(self._pb_id)
            deployments = state.get("deployments")
            for deploy_id in self._deploy_id_list:
                if deployments[deploy_id] == 'FINISHED':
                    deployment_list = txn.list_deployments()
                    if deploy_id in deployment_list:
                        self._deploy.remove(deploy_id)

    def is_sbi_finished(self, txn):
        """Checks if the sbi are finished or cancelled.

        :param txn: config db transaction

        """
        sbi = txn.get_scheduling_block(self._sbi_id)
        status = sbi.get('status')
        if status in ['FINISHED', 'CANCELLED']:
            if status == 'CANCELLED':
                raise Exception('SBI is {}'.format(status))
            self._status = status
            return True

        txn.loop(wait=True)

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
        """Wait loop to check the status of the processing block."""
        for txn in self._config.txn():
            state = txn.get_processing_block_state(self._pb_id)
            pb_status = state.get('status')
            if pb_status in ['FINISHED', 'CANCELLED']:
                if pb_status == 'CANCELLED':
                    raise Exception('PB is {}'.format(pb_status))

            if not txn.is_processing_block_owner(self._pb_id):
                raise Exception("Lost ownership of the processing block")

            yield txn

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Before exiting the phase class, checks if the sbi is marked as
        finished or cancelled for real-time workflows and updates processing
        block state.

        For batch-workflow updates the processing block state.

        """

        if self._workflow_type == 'realtime':
            # Wait until SBI is marked as FINISHED or CANCELLED
            for txn in self.wait_loop():
                if self.is_sbi_finished(txn):
                    if self._deploy_id_list:
                        self._deploy.update_deploy_status('FINISHED')
                    break

            # Clean up deployment.
            LOG.info("Clean up deployment")
            if self._deploy_id_list:
                self.ee_remove()
            self.update_pb_state()
        else:
            self.update_pb_state()

        LOG.info("Deployments All Done")

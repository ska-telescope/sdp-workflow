"""Phase class module for SDP workflow."""
# pylint: disable=too-many-instance-attributes
# pylint: disable=too-many-arguments

import logging

from .dask_deploy import DaskDeploy
from .helm_deploy import HelmDeploy
from .fake_deploy import FakeDeploy

LOG = logging.getLogger("ska_sdp_workflow")


class Phase:
    """
    Workflow phase.

    This should not be created directly, use the
    :func:`ProcessingBlock.create_phase()` method instead.

    :param name: name of the phase
    :type name: str
    :param list_requests: list of requests
    :type list_requests: list
    :param config: SDP configuration client
    :type config: ska_sdp_config.Config
    :param pb_id: processing block ID
    :type pb_id: str
    :param sbi_id: scheduling block instance ID
    :type sbi_id: str
    :param workflow_type: workflow type
    :type workflow_type: str
    """

    def __init__(self, name, list_requests, config, pb_id, sbi_id, workflow_type):
        self._name = name
        self._requests = list_requests
        self._config = config
        self._pb_id = pb_id
        self._sbi_id = sbi_id
        self._workflow_type = workflow_type
        self._deploy_id_list = []
        self._deploy = None
        self._status = None
        self._deployment_status = None

    def __enter__(self):
        """
        Wait for resources to be available.

        While waiting, it checks if the PB is cancelled or finished, and for
        real-time workflows it checks if the SBI is cancelled or finished.
        """

        for txn in self._config.txn():
            self.check_state(txn)

        # Set state to indicate workflow is waiting for resources
        LOG.info("Setting status to WAITING")
        for txn in self._config.txn():
            self.check_state(txn)
            state = txn.get_processing_block_state(self._pb_id)
            state["status"] = "WAITING"
            txn.update_processing_block_state(self._pb_id, state)

        # Wait for resources_available to be true
        LOG.info("Waiting for resources to be available")
        for txn in self._config.txn():
            self.check_state(txn)
            state = txn.get_processing_block_state(self._pb_id)
            r_a = state.get("resources_available")
            if r_a is not None and r_a:
                LOG.info("Resources are available")
                break
            txn.loop(wait=True)

        # Add deployments key to processing block state
        for txn in self._config.txn():
            self.check_state(txn)
            state = txn.get_processing_block_state(self._pb_id)
            if "deployments" not in state:
                state["deployments"] = {}
                txn.update_processing_block_state(self._pb_id, state)

        # Set state to indicate processing has started
        LOG.info("Setting status to RUNNING")
        for txn in self._config.txn():
            self.check_state(txn)
            state = txn.get_processing_block_state(self._pb_id)
            state["status"] = "RUNNING"
            txn.update_processing_block_state(self._pb_id, state)

    def check_state(self, txn):
        """
        Check the state of the processing block.

        Check if the PB is finished or cancelled, and for real-time workflows
        check if the SBI is finished or cancelled.

        :param txn: SDP configuration transaction
        :type txn: ska_sdp_config.Transaction

        """
        LOG.info("Checking PB state")
        pb_state = txn.get_processing_block_state(self._pb_id)
        pb_status = pb_state.get("status")
        if pb_status in ["FINISHED", "CANCELLED"]:
            raise Exception("PB is {}".format(pb_state))

        if not txn.is_processing_block_owner(self._pb_id):
            raise Exception("Lost ownership of the processing block")

        if self._workflow_type == "realtime":
            LOG.info("Checking SBI state")
            sbi = txn.get_scheduling_block(self._sbi_id)
            sbi_status = sbi.get("status")
            if sbi_status in ["FINISHED", "CANCELLED"]:
                raise Exception("PB is {}".format(sbi_status))

    def ee_deploy_test(self, deploy_name, func=None, f_args=None):
        """
        Deploy a fake execution engine.

        This is used for testing and example purposes.

        :param deploy_name: deployment name
        :type deploy_name: str
        :param func: function to execute
        :type func: function
        :param f_args: function arguments
        :type f_args: tuple
        :return: fake execution engine deployment
        :rtype: :class:`FakeDeploy`

        """
        return FakeDeploy(
            self._pb_id, self._config, deploy_name, func=func, f_args=f_args
        )

    def ee_deploy_helm(self, deploy_name, values=None):
        """
        Deploy a Helm execution engine.

        This can be used to deploy any Helm chart.

        :param deploy_name: name of Helm chart
        :type deploy_name: str
        :param values: values to pass to Helm chart
        :type values: dict, optional
        :return: Helm execution engine deployment
        :rtype: :class:`HelmDeploy`

        """
        self._deploy = HelmDeploy(self._pb_id, self._config, deploy_name, values)
        deploy_id = self._deploy.get_id()
        self._deploy_id_list.append(deploy_id)
        return self._deploy

    def ee_deploy_dask(self, name, n_workers, func, f_args):
        """
        Deploy a Dask execution engine.

        :param name: deployment name
        :type name: str
        :param n_workers: number of Dask workers
        :type n_workers: int
        :param func: function to execute
        :type func: function
        :param f_args: function arguments
        :type f_args: tuple
        :return: Dask execution engine deployment
        :rtype: :class:`DaskDeploy`

        """
        return DaskDeploy(self._pb_id, self._config, name, n_workers, func, f_args)

    def ee_remove(self):
        """
        Remove execution engines deployments.
        """
        for txn in self._config.txn():
            state = txn.get_processing_block_state(self._pb_id)
            deployments = state.get("deployments")
            for deploy_id in self._deploy_id_list:
                if deployments[deploy_id] == "FINISHED":
                    deployment_list = txn.list_deployments()
                    if deploy_id in deployment_list:
                        self._deploy.remove(deploy_id)

    def is_sbi_finished(self, txn):
        """
        Check if the SBI is finished or cancelled.

        :param txn: config db transaction
        :type txn: ska_sdp_config.Transaction
        :rtype: bool

        """
        sbi = txn.get_scheduling_block(self._sbi_id)
        status = sbi.get("status")
        if status in ["FINISHED", "CANCELLED"]:
            self._status = status
            if status == "CANCELLED":
                raise Exception("SBI is {}".format(status))
            if self._deploy_id_list:
                self._deploy.update_deploy_status(status)
            return True
        return False

    def update_pb_state(self, status=None):
        """
        Update processing block state.

        If the status is not provided, it is marked as finished.

        :param status: status
        :type status: str, optional

        """
        if status is not None:
            self._status = status

        for txn in self._config.txn():
            # Set state to indicate processing has ended
            state = txn.get_processing_block_state(self._pb_id)
            if self._status is None:
                state["status"] = "FINISHED"
            else:
                LOG.info("Setting PB status to %s", self._status)
                state["status"] = self._status
            txn.update_processing_block_state(self._pb_id, state)

    def wait_loop(self):
        """
        Wait loop to check the status of the processing block.
        """
        for txn in self._config.txn():
            state = txn.get_processing_block_state(self._pb_id)
            pb_status = state.get("status")
            if pb_status in ["FINISHED", "CANCELLED"]:
                if pb_status == "CANCELLED":
                    raise Exception("PB is {}".format(pb_status))

            if not txn.is_processing_block_owner(self._pb_id):
                raise Exception("Lost ownership of the processing block")

            yield txn

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        For real-time workflows, this checks if the SBI is marked as finished
        or cancelled. For both kinds of workflow, it updates the processing
        block state.
        """
        if self._workflow_type == "realtime":

            # Clean up deployment.
            LOG.info("Clean up deployments")
            if self._deploy_id_list:
                self.ee_remove()

        self.update_pb_state()

        LOG.info("Deployments All Done")

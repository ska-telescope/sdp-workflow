"""Execution engine deployment."""


class EEDeploy:
    """
    Base class for execution engine deployment.

    :param pb_id: processing block ID
    :type pb_id: str
    :param config: SDP configuration client
    :type config: ska_sdp_config.Client
    """

    def __init__(self, pb_id, config):
        self._pb_id = pb_id
        self._config = config
        self._deploy_id = None

    def update_deploy_status(self, status):
        """
        Update deployment status.

        :param status: status
        :type status: str

        """
        for txn in self._config.txn():
            state = txn.get_processing_block_state(self._pb_id)
            deployments = state.get("deployments")
            deployments[self._deploy_id] = status
            state["deployments"] = deployments
            txn.update_processing_block_state(self._pb_id, state)

    def get_id(self):
        """
        Get the deployment ID.

        :return: deployment ID
        :rtype: str

        """
        return self._deploy_id

    def remove(self, deploy_id):
        """
        Remove the execution engine.

        :param deploy_id: deployment ID
        :type deploy_id: str

        """
        for txn in self._config.txn():
            deploy = txn.get_deployment(deploy_id)
            txn.delete_deployment(deploy)

    def is_finished(self, txn):
        """
        Check if the deployment is finished.

        :param txn: configuration transaction
        :type txn: ska_sdp_config.Transaction
        :rtype: bool

        """
        state = txn.get_processing_block_state(self._pb_id)
        deployments = state.get("deployments")
        if self._deploy_id in deployments:
            if deployments[self._deploy_id] == "FINISHED":
                deployment_lists = txn.list_deployments()
                if self._deploy_id in deployment_lists:
                    self.remove(self._deploy_id)
                return True
        return False

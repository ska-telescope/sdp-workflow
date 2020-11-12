"""Deploy base class module for SDP Workflow."""


class EEDeploy:
    """Parent Class for Deployments."""
    def __init__(self, pb_id, config):
        """Initialise.

        :param pb_id: processing block ID
        :param config: config DB
        """

        self._pb_id = pb_id
        self._config = config
        self._deploy_id = None

    def update_deploy_status(self, status):
        """Update Deployment Status"""
        for txn in self._config.txn():
            state = txn.get_processing_block_state(self._pb_id)
            deployments = state.get("deployments")
            deployments[self._deploy_id] = status
            state['deployments'] = deployments
            txn.update_processing_block_state(self._pb_id, state)

    def get_id(self):
        """Get deployment id"""
        return self._deploy_id

    def remove(self, deploy_id):
        """Remove Execution Engine."""
        for txn in self._config.txn():
            deploy = txn.get_deployment(deploy_id)
            txn.delete_deployment(deploy)

    def is_finished(self, txn):
        """Checks if the deployments have finished.

        :param txn: config transaction
        """
        state = txn.get_processing_block_state(self._pb_id)
        deployments = state.get("deployments")
        if self._deploy_id in deployments:
            if deployments[self._deploy_id] == 'FINISHED':
                deployment_lists = txn.list_deployments()
                if self._deploy_id in deployment_lists:
                    self.remove(self._deploy_id)
                return True
        return False

"""Helm Deploy class module for SDP Workflow."""

import logging
import ska_sdp_config

from .deploy_base import Deploy

LOG = logging.getLogger('ska_sdp_workflow')


class HelmDeploy(Deploy):
    """Deploy Helm Deploy Execution Engine."""
    def __init__(self, pb_id, config, deploy_name=None):
        """Initialise.

        :param pb_id: processing block ID
        :param config: config DB
        :param deploy_name: deployment name

        """
        super().__init__(pb_id, config)
        self._deploy_flag = False

        self.deploy(deploy_name)

    def deploy(self, deploy_name):
        """Helm Deploy.

        :param deploy_name: deployment name

        """
        LOG.info("Deploying %s Workflow...", deploy_name)
        self._deploy_id = 'proc-{}-{}'.format(self._pb_id, deploy_name)
        self.update_deploy_status('RUNNING')

        chart = {
            'chart': deploy_name,  # Helm chart deploy from the repo
        }
        deploy = ska_sdp_config.Deployment(self._deploy_id,
                                           "helm", chart)
        for txn in self._config.txn():
            txn.create_deployment(deploy)

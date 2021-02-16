"""Helm Deploy class module for SDP Workflow."""

import logging
import ska_sdp_config

from .ee_base_deploy import EEDeploy

LOG = logging.getLogger("ska_sdp_workflow")


class HelmDeploy(EEDeploy):
    """
    Deploy Helm execution engine.

    This should not be created directly, use the :func:`Phase.ee_deploy_helm`
    method instead.

    :param pb_id: processing block ID
    :type pb_id: str
    :param config: SDP configuration client
    :type config: ska_sdp_config.Config
    :param deploy_name: name of Helm chart to deploy
    :type deploy_name: str
    :param values: values to pass to Helm chart
    :type values: dict, optional
    """

    def __init__(self, pb_id, config, deploy_name, values=None):
        super().__init__(pb_id, config)
        self._deploy(deploy_name, values)

    def _deploy(self, deploy_name, values=None):
        """
        Deploy the Helm chart.

        :param deploy_name: deployment name
        :param values: optional dict of values

        """
        LOG.info("Deploying Helm chart: %s", deploy_name)
        self._deploy_id = "proc-{}-{}".format(self._pb_id, deploy_name)
        self.update_deploy_status("RUNNING")

        chart = {
            "chart": deploy_name,  # Helm chart deploy from the repo
        }

        if values is not None:
            chart["values"] = values

        deploy = ska_sdp_config.Deployment(self._deploy_id, "helm", chart)
        for txn in self._config.txn():
            txn.create_deployment(deploy)

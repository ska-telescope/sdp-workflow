"""Dask Deploy class module for SDP Workflow."""
# pylint: disable=too-many-arguments
# pylint: disable=broad-except


import os
import sys
import logging
import threading
import distributed
import ska_sdp_config

from .deploy_base import Deploy

LOG = logging.getLogger('ska_sdp_workflow')


class DaskDeploy(Deploy):
    """Deploy Dask Execution Engine."""
    def __init__(self, pb_id, config, deploy_name, n_workers,
                 func, f_args):
        """Initialise.

        :param pb_id: processing block ID
        :param config: config DB
        :param deploy_name: deployment name
        :param func: function to process
        :param f_args: function arguments
        :param n_workers: number of dask workers

        """
        super().__init__(pb_id, config)
        thread = threading.Thread(target=self.deploy_dask,
                                  args=(deploy_name, n_workers, func, f_args,))
        thread.setDaemon(True)
        thread.start()

    def deploy_dask(self, deploy_name, n_workers, func, f_args):
        """Deploy Dask EE.

        :param deploy_name: deployment name
        :param func: function to process
        :param f_args: function arguments
        :param n_workers: number of dask workers

        """
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

        # Set Deployment to RUNNING status in the config_db
        self.update_deploy_status('RUNNING')

        deploy = ska_sdp_config.Deployment(
            self._deploy_id, "helm", {
                'chart': 'dask/dask',
                'values': {
                    'jupyter.enabled': 'false',
                    'jupyter.rbac': 'false',
                    'worker.replicas': n_workers,
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
            except Exception as ex:
                LOG.error(ex)
        if client is None:
            LOG.error("Could not connect to Dask!")
            sys.exit(1)
        LOG.info("Connected to Dask")

        # Doing some silly calculation
        result = func(*f_args)
        compute_result = result.compute()
        LOG.info("Computed Result %s", compute_result)

        # Update Deployment Status
        self.update_deploy_status('FINISHED')

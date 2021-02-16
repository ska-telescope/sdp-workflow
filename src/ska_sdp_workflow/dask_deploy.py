"""Dask execution engine deployment module."""
# pylint: disable=too-many-arguments
# pylint: disable=broad-except

import os
import sys
import logging
import threading
import distributed
import ska_sdp_config

from .ee_base_deploy import EEDeploy

LOG = logging.getLogger("ska_sdp_workflow")


class DaskDeploy(EEDeploy):
    """
    Deploy a Dask execution engine.

    The function when called with the arguments should return a Dask graph. The
    graph is then executed by calling the compute method:

    .. code-block:: python

        result = func(*f_args)
        result.compute()

    This happens in a separate thread so the constructor can return
    immediately.

    This should not be created directly, use the :func:`Phase.ee_deploy_dask`
    method instead.

    :param pb_id: processing block ID
    :type pb_id: str
    :param config: configuration DB client
    :type config: ska_sdp_config.Client
    :param deploy_name: deployment name
    :type deploy_name: str
    :param n_workers: number of Dask workers
    :type n_workers: int
    :param func: function to execute
    :type func: function
    :param f_args: function arguments
    :type f_args: tuple
    """

    def __init__(self, pb_id, config, deploy_name, n_workers, func, f_args):
        super().__init__(pb_id, config)
        thread = threading.Thread(
            target=self._deploy,
            args=(
                deploy_name,
                n_workers,
                func,
                f_args,
            ),
            daemon=True,
        )
        thread.start()

    def _deploy(self, deploy_name, n_workers, func, f_args):
        """
        Make the deployment and execute the function.

        This is called from the thread.

        :param deploy_name: deployment name
        :param func: function to process
        :param f_args: function arguments
        :param n_workers: number of dask workers

        """
        LOG.info("Deploying Dask...")
        self._deploy_id = "proc-{}-{}".format(self._pb_id, deploy_name)
        LOG.info(self._deploy_id)

        # Set Deployment to RUNNING status in the config_db
        self.update_deploy_status("RUNNING")

        deploy = ska_sdp_config.Deployment(
            self._deploy_id,
            "helm",
            {
                "chart": "dask/dask",
                "values": {
                    "jupyter.enabled": "false",
                    "jupyter.rbac": "false",
                    "worker.replicas": n_workers,
                    # We want to access Dask in-cluster using a DNS name
                    "scheduler.serviceType": "ClusterIP",
                    "worker.image.tag": distributed.__version__,
                    "scheduler.image.tag": distributed.__version__,
                },
            },
        )

        for txn in self._config.txn():
            txn.create_deployment(deploy)

        LOG.info("Waiting for Dask...")
        client = None

        for _ in range(200):
            try:
                client = distributed.Client(
                    self._deploy_id
                    + "-scheduler."
                    + os.environ["SDP_HELM_NAMESPACE"]
                    + ":8786"
                )
            except Exception as ex:
                LOG.error(ex)
        if client is None:
            LOG.error("Could not connect to Dask!")
            sys.exit(1)
        LOG.info("Connected to Dask")

        # Computing result
        result = func(*f_args)
        compute_result = result.compute()
        LOG.info("Computed Result %s", compute_result)

        # Update Deployment Status
        self.update_deploy_status("FINISHED")

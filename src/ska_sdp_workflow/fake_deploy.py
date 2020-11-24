"""Fake deployment."""
# pylint: disable=too-many-arguments

import logging
import threading

from .ee_base_deploy import EEDeploy

LOG = logging.getLogger('ska_sdp_workflow')


class FakeDeploy(EEDeploy):
    """
    Deploy a fake execution engine.

    The function is called with the arguments in a separate thread so the
    constructor can return immediately.

    This should not be created directly, use the :func:`Phase.ee_deploy_test`
    method instead.

    :param pb_id: processing block ID
    :type pb_id: str
    :param config: SDP configuration client
    :type config: ska_sdp_config.Client
    :param deploy_name: deployment name
    :type deploy_name: str
    :param func: function to execute
    :type func: function
    :param f_args: function arguments
    :type f_args: tuple

    """
    def __init__(self, pb_id, config, deploy_name,
                 func=None, f_args=None,):
        super().__init__(pb_id, config)
        thread = threading.Thread(target=self._deploy,
                                  args=(deploy_name, func, f_args,),
                                  daemon=True)
        thread.start()

    def _deploy(self, deploy_name, func=None, f_args=None):
        """
        Execute the function.

        This is called by the execution thread.

        :param deploy_name: deployment name
        :param func: function to process
        :param f_args: function arguments

        """
        LOG.info("Deploying %s Workflow...", deploy_name)
        self._deploy_id = 'proc-{}-{}'.format(self._pb_id, deploy_name)
        self.update_deploy_status('RUNNING')

        LOG.info("Starting processing for %fs", *f_args)
        func(*f_args)
        LOG.info('Finished processing')
        self.update_deploy_status('FINISHED')

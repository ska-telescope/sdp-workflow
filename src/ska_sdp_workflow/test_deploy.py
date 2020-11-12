"""Test Deploy class module for SDP Workflow."""
# pylint: disable=too-many-arguments

import logging
import threading

from .deploy_base import EEDeploy

LOG = logging.getLogger('ska_sdp_workflow')


class TestDeploy(EEDeploy):
    """Deploy a Fake Execution Engine."""
    def __init__(self, pb_id, config, deploy_name,
                 func=None, f_args=None,):
        """Initialise.

        :param pb_id: processing block ID
        :param config: config DB
        :param deploy_name: deployment name
        :param func: function to process
        :param f_args: function arguments

        """
        super().__init__(pb_id, config)

        thread = threading.Thread(target=self.deploy,
                                  args=(deploy_name, func, f_args,),
                                  daemon=True)
        thread.start()

    def deploy(self, deploy_name, func=None, f_args=None):
        """Test Deploy.

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

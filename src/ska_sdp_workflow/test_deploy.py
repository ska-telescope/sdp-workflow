"""Test Deploy class module for SDP Workflow."""
# pylint: disable=too-many-arguments

import logging

from .deploy_base import Deploy

LOG = logging.getLogger('ska_sdp_workflow')


class TestDeploy(Deploy):
    """Deploy a fake Execution Engine."""
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

        self.deploy(deploy_name, func, f_args)

    def deploy(self, deploy_name, func=None, f_args=None):
        """Helm Deploy.

        :param deploy_name: deployment name
        :param func: function to process
        :param f_args: function arguments

        """
        LOG.info("Deploying %s Workflow...", deploy_name)
        self._deploy_id = 'proc-{}-{}'.format(self._pb_id, deploy_name)
        self.update_deploy_status('RUNNING')

        if deploy_name == 'test_batch':
            LOG.info("Starting processing for %fs", *f_args)
            func(*f_args)
            LOG.info('Finished processing')
            self.update_deploy_status('FINISHED')
        else:
            LOG.info("Pretending to deploy execution engine.")

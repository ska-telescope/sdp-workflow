"""Buffer request class module for SDP Workflow."""
# pylint: disable=too-few-public-methods

import logging
import ska_sdp_config

LOG = logging.getLogger('ska_sdp_workflow')


class BufferRequest:
    """
    Request a buffer reservation.

    :param config: handle to a ska-sdp-config.Config instance
    :param pd_id: str
    :param size: size of the buffer - str as used in Kubernetes PersistentVolumes
    :type size: float
    :param tags: tags describing the type of buffer required
    :type tags: list of str
    """

    def __init__(self, name, size, tags, config, pb_id):
        LOG.info("Buffer request, name=%s, size: %s, tags: %s", name, size, tags)

        # Use of the tags is yet to be decided - just save for now!
        self.tags = tags

        if name:
            deploy_id = "buff-{}".format(name)
        else:
            deploy_id = "buff-{}".format(pb_id)
        values = {}
        if size:
            values['size'] = size
        LOG.info("Buffer deployment: %s", deploy_id)
        deploy = ska_sdp_config.Deployment(
             deploy_id, 'helm', {'chart': 'buffer', 'values': values}
        )
        self.deployment = deploy
        for txn in config.txn():
            depls = txn.list_deployments(prefix='buff')
            LOG.info('Deployments.....%s', depls)
            if deploy_id in depls:
                LOG.info("Using existing deployment")
                self.deployment = txn.get_deployment(deploy_id)
            else:
                LOG.info("Creating new deployment")
                txn.create_deployment(deploy)

    def __repr__(self):
        return self.deployment.id

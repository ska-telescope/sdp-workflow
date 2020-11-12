"""Buffer Request class module for SDP Workflow."""
# pylint: disable=too-few-public-methods

import logging

LOG = logging.getLogger('ska_sdp_workflow')


class BufferRequest:
    """Buffer request class. Currently just a placeholder."""

    def __init__(self, size, tags):
        """Initialise

        :param size: size of the buffer
        :param tags: type of the buffer
        """
        LOG.info("For %s Buffer Requested size - %s", tags, size)

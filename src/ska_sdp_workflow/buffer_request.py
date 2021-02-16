"""Buffer request class module for SDP Workflow."""
# pylint: disable=too-few-public-methods

import logging

LOG = logging.getLogger("ska_sdp_workflow")


class BufferRequest:
    """
    Request a buffer reservation.

    This is currently just a placeholder.

    :param size: size of the buffer
    :type size: float
    :param tags: tags describing the type of buffer required
    :type tags: list of str
    """

    def __init__(self, size, tags):
        LOG.info("Buffer request, size: %s, tags: %s", size, tags)

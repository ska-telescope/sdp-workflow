"""Pytest fixtures."""

# pylint: disable=redefined-outer-name

import pytest

from ska_sdp_workflow import workflow


# Use the config DB memory backend in the subarray. This will be overridden if
# the TOGGLE_CONFIG_DB environment variable is set to 1.
workflow.FEATURE_CONFIG_DB.set_default(False)

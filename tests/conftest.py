"""Pytest fixtures."""

# pylint: disable=redefined-outer-name

from ska_sdp_workflow import workflow


# Use the config DB memory backend. This will be overridden if the
# FEATURE_CONFIG_DB environment variable is set to 1.
workflow.FEATURE_CONFIG_DB.set_default(False)

"""SDP Workflow tests."""

# pylint: disable=redefined-outer-name
# pylint: disable=duplicate-code

import pytest
from pytest_bdd import (given, parsers, scenarios, then, when)

from src.ska_sdp_workflow import ProcessingBlock
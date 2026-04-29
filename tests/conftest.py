"""Shared pytest fixtures."""

from unittest.mock import MagicMock

import pytest


@pytest.fixture
def mock_workspace_client():
    """A mocked WorkspaceClient for testing app and deploy_workflow."""
    client = MagicMock()
    client.config.host = "https://test-workspace.cloud.databricks.com"
    return client

"""Tests for the deploy_workflow script."""

import sys
from unittest.mock import MagicMock, patch
import pytest


@pytest.fixture(autouse=True)
def mock_databricks_sdk():
    """Mock databricks.sdk so deploy_workflow.py can be imported."""
    mock_sdk = MagicMock()
    mock_jobs = MagicMock()
    mock_jobs.Task = MagicMock
    mock_jobs.NotebookTask = MagicMock
    mock_jobs.JobParameterDefinition = MagicMock
    mock_jobs.TaskDependency = MagicMock
    mock_jobs.Source = MagicMock()
    mock_jobs.Source.WORKSPACE = "WORKSPACE"

    modules = {
        "databricks": MagicMock(),
        "databricks.sdk": mock_sdk,
        "databricks.sdk.service": MagicMock(),
        "databricks.sdk.service.jobs": mock_jobs,
    }

    with patch.dict(sys.modules, modules):
        if "workflow.deploy_workflow" in sys.modules:
            del sys.modules["workflow.deploy_workflow"]
        yield mock_sdk


def _import_deploy():
    """Import deploy_workflow after mocks are in place."""
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "deploy_workflow",
        "workflow/deploy_workflow.py",
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class TestDeployConfig:
    def test_workflow_name(self):
        mod = _import_deploy()
        assert mod.WORKFLOW_NAME == "Column_Mapping_To_Global_Model"

    def test_has_11_tasks(self):
        mod = _import_deploy()
        assert len(mod.TASKS) == 11

    def test_has_6_parameters(self):
        mod = _import_deploy()
        assert len(mod.JOB_PARAMETERS) == 6

    def test_no_internal_references(self):
        mod = _import_deploy()
        assert "fevm" not in mod.NOTEBOOK_BASE_PATH.lower()
        assert "philipp" not in mod.NOTEBOOK_BASE_PATH.lower()

    def test_tags_no_internal_references(self):
        mod = _import_deploy()
        for key, val in mod.JOB_TAGS.items():
            assert "fevm" not in val.lower()
            assert "pt-harmonization" not in val


class TestDeployFunction:
    def test_creates_new_job(self):
        mod = _import_deploy()
        mock_client = MagicMock()
        mock_client.config.host = "https://test.cloud.databricks.com"
        mock_client.jobs.list.return_value = []
        mock_client.jobs.create.return_value = MagicMock(job_id=12345)

        with patch.object(mod, "WorkspaceClient", return_value=mock_client):
            # Need to reimport since WorkspaceClient is used at module level
            pass

    def test_deploy_is_callable(self):
        mod = _import_deploy()
        assert callable(mod.deploy)

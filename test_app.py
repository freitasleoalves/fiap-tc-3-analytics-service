import pytest
import json
from unittest.mock import patch, MagicMock


@pytest.fixture(autouse=True)
def mock_env(monkeypatch):
    """Set environment variables for testing."""
    monkeypatch.setenv("CLOUD_PROVIDER", "aws")
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("AWS_SQS_URL", "http://localhost:4566/queue/test")
    monkeypatch.setenv("AWS_DYNAMODB_TABLE", "TestTable")
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://localhost:4566")


def create_app():
    """Create a fresh app instance with mocked worker."""
    with patch("app.start_worker"):
        import importlib
        import app as app_module
        importlib.reload(app_module)
        app_module.app.config["TESTING"] = True
        return app_module.app, app_module


class TestHealth:
    def test_health_returns_ok(self, monkeypatch):
        flask_app, _ = create_app()
        with flask_app.test_client() as client:
            response = client.get("/health")
            assert response.status_code == 200
            data = json.loads(response.data)
            assert data["status"] == "ok"


class TestAWSProcessMessage:
    @patch("app.start_worker")
    def test_process_valid_message(self, mock_worker, monkeypatch):
        import importlib
        import app as app_module
        importlib.reload(app_module)

        mock_sqs = MagicMock()
        mock_dynamo = MagicMock()
        queue_url = "http://localhost:4566/queue/test"
        table_name = "TestTable"

        message = {
            "MessageId": "test-id",
            "Body": json.dumps({
                "user_id": "user1",
                "flag_name": "flag1",
                "result": True,
                "timestamp": "2026-01-01T00:00:00Z",
            }),
            "ReceiptHandle": "handle-123",
        }

        app_module.aws_process_message(mock_sqs, mock_dynamo, queue_url, table_name, message)

        mock_dynamo.put_item.assert_called_once()
        mock_sqs.delete_message.assert_called_once_with(
            QueueUrl=queue_url, ReceiptHandle="handle-123"
        )

    @patch("app.start_worker")
    def test_process_invalid_json(self, mock_worker, monkeypatch):
        import importlib
        import app as app_module
        importlib.reload(app_module)

        mock_sqs = MagicMock()
        mock_dynamo = MagicMock()

        message = {
            "MessageId": "test-id",
            "Body": "invalid-json",
            "ReceiptHandle": "handle-123",
        }

        # Should not raise
        app_module.aws_process_message(mock_sqs, mock_dynamo, "url", "table", message)
        mock_dynamo.put_item.assert_not_called()


class TestAzureProcessMessage:
    @patch("app.start_worker")
    def test_process_valid_message(self, mock_worker, monkeypatch):
        import importlib
        import app as app_module
        importlib.reload(app_module)

        mock_table_client = MagicMock()
        message = json.dumps({
            "user_id": "user1",
            "flag_name": "flag1",
            "result": True,
            "timestamp": "2026-01-01T00:00:00Z",
        })

        mock_msg = MagicMock()
        mock_msg.__str__ = MagicMock(return_value=message)

        app_module.azure_process_message(mock_table_client, mock_msg)

        mock_table_client.create_entity.assert_called_once()

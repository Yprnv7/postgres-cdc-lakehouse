"""Tests for OpenLineage event construction and emission."""

from unittest.mock import MagicMock, patch

from consumer.sink_worker import CDCSinkWorker
from consumer.storage import LakehouseStorage


def test_openlineage_event_payload_structure():
    """Verify that emit_openlineage_event produces valid OpenLineage 1.0.5 structure."""
    storage = LakehouseStorage(db_path=":memory:")
    worker = CDCSinkWorker(storage=storage)

    event = worker.emit_openlineage_event(event_type="COMPLETE")

    assert event["eventType"] == "COMPLETE"
    assert "eventTime" in event
    assert "run" in event
    assert "runId" in event["run"]
    assert event["job"]["namespace"] == "postgres_cdc_pipeline"
    assert event["job"]["name"] == "cdc_ingestion_sink"

    # Verify input datasets
    input_names = [d["name"] for d in event["inputs"]]
    assert "oltp_db.public.customers" in input_names
    assert "oltp_db.public.orders" in input_names

    # Verify output datasets
    output_names = [d["name"] for d in event["outputs"]]
    assert "lakehouse.dim_customers_scd2" in output_names
    assert "lakehouse.fct_orders" in output_names

    assert event["producer"] == "https://github.com/Yprnv7/postgres-cdc-lakehouse"
    storage.close()


def test_openlineage_http_post_dispatch():
    """Verify that when MARQUEZ_URL is provided, an HTTP POST request is sent."""
    storage = LakehouseStorage(db_path=":memory:")
    worker = CDCSinkWorker(storage=storage)

    with patch("httpx.post") as mock_post:
        mock_resp = MagicMock()
        mock_resp.status_code = 201
        mock_post.return_value = mock_resp

        event = worker.emit_openlineage_event(
            event_type="COMPLETE",
            marquez_url="http://localhost:5000",
        )

        assert mock_post.called
        call_url = mock_post.call_args[0][0]
        assert call_url == "http://localhost:5000/api/v1/lineage"
        call_json = mock_post.call_args[1]["json"]
        assert call_json["run"]["runId"] == event["run"]["runId"]

    storage.close()

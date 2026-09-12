import logging
import pytest
from unittest import mock

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.utils.bigquery_get_data import bigquery_get_data

class TestBigqueryGetData:
    def test_yields_successive_batches(self, caplog):
        """
        Successive batches are yielded as lists of row values, with start_index
        advancing by batch_size on each call to BigQueryHook.list_rows.
        The generator terminates when a batch comes back empty.
        selected_fields and batch_size are passed through to list_rows unmodified.
        """
        mock_hook = mock.MagicMock(spec=BigQueryHook)

        # Simulate BigQuery Row objects returning dictionary values
        mock_row_1 = mock.MagicMock()
        mock_row_1.values.return_value = {"col1": "A"}
        mock_row_2 = mock.MagicMock()
        mock_row_2.values.return_value = {"col1": "B"}

        # Return two batches of 1, then an empty list to terminate
        mock_hook.list_rows.side_effect = [
            [mock_row_1],
            [mock_row_2],
            []
        ]

        logger = logging.getLogger(__name__)

        results = list(bigquery_get_data(
            logger=logger,
            dataset_id="my_dataset",
            table_id="my_table",
            big_query_hook=mock_hook,
            batch_size=1,
            selected_fields=["col1"]
        ))

        assert results == [
            [{"col1": "A"}],
            [{"col1": "B"}]
        ]

        # Verify list_rows was called 3 times and start_index advanced correctly
        assert mock_hook.list_rows.call_count == 3
        mock_hook.list_rows.assert_has_calls([
            mock.call(dataset_id="my_dataset", table_id="my_table", max_results=1, selected_fields=["col1"], start_index=0),
            mock.call(dataset_id="my_dataset", table_id="my_table", max_results=1, selected_fields=["col1"], start_index=1),
            mock.call(dataset_id="my_dataset", table_id="my_table", max_results=1, selected_fields=["col1"], start_index=2),
        ])

        # Verify logging assertions via structured caplog checks
        assert "Job Finished" in caplog.text

    def test_raises_type_error_for_row_iterator(self):
        """A RowIterator return raises TypeError (guards against return_iterator=True)."""
        mock_hook = mock.MagicMock(spec=BigQueryHook)

        # A real RowIterator doesn't have a __len__ method. We simulate this by having len() raise TypeError
        mock_iterator = mock.MagicMock()
        type(mock_iterator).__len__ = mock.PropertyMock(side_effect=TypeError("object of type 'RowIterator' has no len()"))
        mock_hook.list_rows.return_value = mock_iterator

        logger = logging.getLogger(__name__)

        with pytest.raises(TypeError):
            # Using list() consumes the generator and triggers the len(rows) check which will raise
            list(bigquery_get_data(
                logger=logger,
                dataset_id="my_dataset",
                table_id="my_table",
                big_query_hook=mock_hook,
                batch_size=1,
                selected_fields=["col1"]
            ))

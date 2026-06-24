from __future__ import annotations
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture(autouse=True)
def mock_psycopg2_connect():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    mock_cursor.fetchone.return_value = (False,)

    with patch("psycopg2.connect", return_value=mock_conn) as mock_connect:
        yield mock_connect

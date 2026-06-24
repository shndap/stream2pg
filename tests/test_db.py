from __future__ import annotations

from datetime import datetime

import pytest
from stream2pg.db import convert_value, infer_sql_type


class TestInferSqlType:
    def test_boolean(self):
        assert infer_sql_type(True) == "BOOLEAN"
        assert infer_sql_type(False) == "BOOLEAN"

    def test_integer(self):
        assert infer_sql_type(42) == "BIGINT"
        assert infer_sql_type(-1) == "BIGINT"

    def test_float(self):
        assert infer_sql_type(3.14) == "DOUBLE PRECISION"

    def test_string_timestamp(self):
        assert infer_sql_type("2024-01-01T00:00:00") == "TIMESTAMP"
        assert infer_sql_type("2024-01-01T00:00:00Z") == "TIMESTAMP"

    def test_string_text(self):
        assert infer_sql_type("hello") == "TEXT"
        assert infer_sql_type("not a date") == "TEXT"

    def test_none(self):
        assert infer_sql_type(None) == "TEXT"


class TestConvertValue:
    def test_string_timestamp_with_z(self):
        result = convert_value("2024-01-01T00:00:00Z")
        assert isinstance(result, datetime)

    def test_string_timestamp_with_offset(self):
        result = convert_value("2024-01-01T00:00:00+00:00")
        assert isinstance(result, datetime)

    def test_string_regular(self):
        result = convert_value("hello")
        assert result == "hello"

    def test_non_string(self):
        assert convert_value(42) == 42
        assert convert_value(3.14) == 3.14
        assert convert_value(True) is True

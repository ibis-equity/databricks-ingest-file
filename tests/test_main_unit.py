import unittest
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

from main import (
    batched,
    directory_depth,
    quote_ident,
    sanitize_column_name,
    sql_literal,
    to_sql_type,
    within_depth,
)


class MainPureFunctionTests(unittest.TestCase):
    def test_sanitize_column_name_normalizes_symbols_spaces_and_case(self) -> None:
        self.assertEqual(sanitize_column_name(" Customer Name "), "customer_name")
        self.assertEqual(sanitize_column_name("Revenue($)"), "revenue")
        self.assertEqual(sanitize_column_name("A---B"), "a_b")

    def test_sanitize_column_name_falls_back_to_col_for_empty(self) -> None:
        self.assertEqual(sanitize_column_name("   "), "col")
        self.assertEqual(sanitize_column_name("!!!"), "col")

    def test_quote_ident_quotes_each_segment(self) -> None:
        self.assertEqual(quote_ident("schema.table"), "`schema`.`table`")
        self.assertEqual(quote_ident("single"), "`single`")

    def test_to_sql_type_maps_supported_types(self) -> None:
        self.assertEqual(to_sql_type("int64"), "BIGINT")
        self.assertEqual(to_sql_type("float64"), "DOUBLE")
        self.assertEqual(to_sql_type("bool"), "BOOLEAN")
        self.assertEqual(to_sql_type("datetime64[ns]"), "TIMESTAMP")
        self.assertEqual(to_sql_type("object"), "STRING")

    def test_sql_literal_handles_core_value_types(self) -> None:
        self.assertEqual(sql_literal(None), "NULL")
        self.assertEqual(sql_literal(True), "TRUE")
        self.assertEqual(sql_literal(False), "FALSE")
        self.assertEqual(sql_literal(42), "42")
        self.assertEqual(sql_literal(Decimal("12.34")), "12.34")
        self.assertEqual(sql_literal("O'Hare"), "'O''Hare'")

    def test_sql_literal_formats_date_and_datetime(self) -> None:
        ts = datetime(2026, 6, 11, 15, 30, 45, tzinfo=UTC)
        self.assertEqual(sql_literal(ts), "TIMESTAMP '2026-06-11 15:30:45'")
        self.assertEqual(sql_literal(date(2026, 6, 11)), "DATE '2026-06-11'")

    def test_directory_depth_and_within_depth(self) -> None:
        root = Path("C:/tmp/inbox")
        child0 = root / "file.csv"
        child1 = root / "nested" / "file.csv"
        outside = Path("C:/tmp/other/file.csv")

        self.assertEqual(directory_depth(root, child0), 0)
        self.assertEqual(directory_depth(root, child1), 1)
        self.assertIsNone(directory_depth(root, outside))

        self.assertTrue(within_depth(root, child0, 0))
        self.assertTrue(within_depth(root, child1, 1))
        self.assertFalse(within_depth(root, child1, 0))
        self.assertFalse(within_depth(root, outside, 5))

    def test_batched_splits_iterables(self) -> None:
        chunks = list(batched([1, 2, 3, 4, 5], 2))
        self.assertEqual(chunks, [[1, 2], [3, 4], [5]])


if __name__ == "__main__":
    unittest.main()


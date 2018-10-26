import unittest
from base_mysql_importer import BaseMysqlImporter
from unittest.mock import MagicMock

class TestBaseMysqlImporter(unittest.TestCase):
    def setUp(self):
        self.subject = BaseMysqlImporter()
        self.subject.db_connection = MagicMock()
        self.subject.db_cursor = MagicMock()
        self.subject.db_cursor.execute = MagicMock()

    def test_get_inserted_ids(self):
        self.subject.db_cursor.fetchall = MagicMock(return_value=[(1,),(2,),(3,)])
        result = self.subject.get_inserted_ids('the_table')
        self.subject.db_cursor.execute.assert_called_with('SELECT id FROM the_table')
        self.subject.db_cursor.fetchall.assert_called()
        self.assertListEqual(result, [1,2,3])

    def test_setup_db_without_existing_index(self):
        self.subject.connect_db = MagicMock()
        self.subject.db_cursor.fetchall = MagicMock(return_value=[(0,)])
        self.subject.setup_db()
        self.subject.db_cursor.execute.assert_called_with(
            'CREATE UNIQUE INDEX import_export_unique on import_export(country_id, product_id, year)'
        )
        self.assertEqual(self.subject.db_cursor.execute.call_count, 6)

    def test_setup_db_with_existing_index(self):
        self.subject.connect_db = MagicMock()
        self.subject.db_cursor.fetchall = MagicMock(return_value=[(1,)])
        self.subject.setup_db()
        self.assertEqual(self.subject.db_cursor.execute.call_count, 5)

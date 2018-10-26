import unittest
from base_mysql_importer import BaseMysqlImporter
import mysql.connector as sql

class IntegrationTestBaseMysqlImporter(unittest.TestCase):
    def setUp(self):
        self.subject = BaseMysqlImporter(file='data/small.csv', database_name='forestry_test')

    def test_full_import(self):
        self.subject.run()
        db_connection = sql.connect(
            host='localhost', user='root', password='', charset='utf8mb4', database='forestry_test'
        )
        db_cursor = db_connection.cursor()
        db_cursor.execute('SELECT COUNT(*) FROM countries')
        result = db_cursor.fetchall()
        self.assertEqual(result[0][0], 2)

        db_cursor.execute('SELECT COUNT(*) FROM products')
        result = db_cursor.fetchall()
        self.assertEqual(result[0][0], 1)

        db_cursor.execute('SELECT COUNT(*) FROM import_export')
        result = db_cursor.fetchall()
        self.assertEqual(result[0][0], 2)

        db_cursor.execute(
            'SELECT import_value, export_value, production FROM import_export WHERE country_id=2'
        )
        result = db_cursor.fetchall()
        self.assertEqual(len(result), 1)
        self.assertTupleEqual(result[0], (301, 302, None))

        db_cursor.close()
        db_connection.close()


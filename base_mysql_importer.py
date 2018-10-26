''' Setup MySQL DB and tables and import CSV file into MySQL.'''
import csv
import mysql.connector as sql

class BaseMysqlImporter:
    FILE = 'data/forestry.csv'

    VALUE_TYPE_MAP = {
        'Import Value': 'import_value',
        'Import Quantity': 'import',
        'Production': 'production',
        'Export Value': 'export_value',
        'Export Quantity': 'export'
    }

    def __init__(self, file=FILE, database_name='forestry'):
        self.db_cursor = None
        self.db_connection = None
        self.file = file
        self.database_name = database_name

    def run(self):
        self.setup_db()
        generator = self.csv_rows_generator()
        self.insert_in_dataset(generator)
        self.close_db()

    def csv_rows_generator(self):
        with open(self.file, 'r') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                yield row

    def get_inserted_ids(self, table):
        self.db_cursor.execute('SELECT id FROM %s' % table)
        result = self.db_cursor.fetchall()
        return list(map(lambda x: x[0], result))

    def insert_in_dataset(self, rows):
        inserted_country_ids = self.get_inserted_ids('countries')
        inserted_product_ids = self.get_inserted_ids('products')

        i = 0
        for row in rows:
            if row[0] == 'Area Code':
                continue
            print(row)
            country_id = int(row[0])
            product_id = int(row[2])
            year = int(row[7])
            if country_id not in inserted_country_ids:
                self.db_cursor.execute(
                    'INSERT INTO countries VALUES (%s, %s)', (int(row[0]), row[1])
                )
                inserted_country_ids.append(country_id)
            if product_id not in inserted_product_ids:
                self.db_cursor.execute(
                    'INSERT INTO products VALUES (%s, %s)', (int(row[2]), row[3])
                )
                inserted_product_ids.append(product_id)

            value_type = self.VALUE_TYPE_MAP[row[5]]
            value = float(row[9])
            if value_type is not None:
                query = (
                    'INSERT INTO import_export(country_id, product_id, year, `%s`) '
                    'VALUES (%s, %s, %s, %s) ON DUPLICATE KEY UPDATE `%s` = %s'
                    % (value_type, country_id, product_id, year, value, value_type, value)
                )
                self.db_cursor.execute(query)

            i += 1
            if i%100 == 99:
                self.db_connection.commit()

        self.db_connection.commit()

    def connect_db(self, database=None):
        self.db_connection = sql.connect(
            host='localhost', user='root', password='', charset='utf8mb4', database=database
        )
        self.db_cursor = self.db_connection.cursor()

    def close_db(self):
        self.db_cursor.close()
        self.db_connection.close()

    def setup_db(self):
        self.connect_db()
        query = 'CREATE DATABASE IF NOT EXISTS %s' % (self.database_name)
        self.db_cursor.execute(query)
        self.db_connection.commit()
        self.close_db()
        self.connect_db(database=self.database_name)
        self.setup_tables()

    def setup_tables(self):
        self.db_cursor.execute(
            'CREATE TABLE IF NOT EXISTS countries ('
            'id INT NOT NULL UNIQUE, '
            'name VARCHAR(200), '
            'PRIMARY KEY(id)) '
            'CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci'
        )
        self.db_cursor.execute(
            'CREATE TABLE IF NOT EXISTS products ('
            'id INT NOT NULL UNIQUE, '
            'name VARCHAR(200), '
            'PRIMARY KEY(id)) '
            'CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci'
        )
        self.db_cursor.execute(
            'CREATE TABLE IF NOT EXISTS import_export ('
            'id INT NOT NULL UNIQUE AUTO_INCREMENT, '
            'country_id INT NOT NULL, '
            'product_id INT NOT NULL, '
            'year INT NOT NULL, '
            'production FLOAT, '
            'export FLOAT, '
            'import FLOAT, '
            'export_value FLOAT, '
            'import_value FLOAT, '
            'PRIMARY KEY(id)) '
            'CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci'
        )
        self.db_connection.commit()

        self.db_cursor.execute(
            'SELECT COUNT(1) index_is_there FROM INFORMATION_SCHEMA.STATISTICS '
            'WHERE table_schema=DATABASE() '
            "AND table_name='import_export' "
            "AND index_name='import_export_unique'"
        )
        result = self.db_cursor.fetchall()
        if int(result[0][0]) == 0:
            self.db_cursor.execute(
                'CREATE UNIQUE INDEX import_export_unique on '
                'import_export(country_id, product_id, year)'
            )
            self.db_connection.commit()

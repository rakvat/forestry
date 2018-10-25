import csv
import dataset
import mysql.connector as sql

class DatasetImporter:
    #FILE = 'data/forestry.csv'
    FILE = 'data/small.csv'

    def run(self):
        print('DatasetImporter')
        self.setup_db()
        generator = self.csv_rows_generator()
        self.insert_in_dataset(generator)

    def setup_db(self):
        # dataset can't create a database so we still need mysql-connection
        db_connection = sql.connect(host='localhost', user='root', password='', charset='utf8mb4')
        db_cursor = db_connection.cursor()
        db_cursor.execute('CREATE DATABASE IF NOT EXISTS forestry_dataset')
        db_connection.commit()

        db = dataset.connect(url='mysql://root@127.0.0.1:3306/forestry_dataset')
        self.countries = db['countries']
        self.products = db['products']
        self.import_export = db['import_export']

    def csv_rows_generator(self):
        with open(self.FILE, 'r') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                yield row

    def insert_in_dataset(self, rows):
        for row in rows:
            if row[0] == 'Area Code': continue
            self.countries.insert_ignore({'id': int(row[0]), 'name': row[1]}, ['id'])
            print(row)


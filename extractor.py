import mysql.connector as sql

class Extractor:

    def __init__(self):
      self.connect_db(database='forestry')

    def generate(self):
      self.db_cursor.execute(
          'SELECT countries.name AS country, products.name AS product, year, import, import_value, export, export_value '
          'FROM import_export '
          'LEFT JOIN products ON import_export.product_id=products.id '
          'LEFT JOIN countries ON import_export.country_id=countries.id '
          'WHERE product_id=1675 AND country_id=67 '
          'ORDER BY year;'
      )
      result = self.db_cursor.fetchall()
      for row in result:
          yield row


    def connect_db(self, database=None):
        self.db_connection = sql.connect(
            host='localhost', user='root', password='', charset='utf8mb4', database=database
        )
        self.db_cursor = self.db_connection.cursor()


from base_mysql_importer import BaseMysqlImporter
from dataset_importer import DatasetImporter

def main():
    base_mysql_importer = BaseMysqlImporter()
    base_mysql_importer.run()

    dataset_importer = DatasetImporter()
    dataset_importer.run()

if __name__ == '__main__':
    main()

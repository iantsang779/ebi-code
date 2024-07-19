import argparse
import mysql.connector 
import pandas as pd
from mysql.connector import Error

class Database:
    
    """ Initialize connection to MySQL server """
    def __init__(self, host, user, port, password, remove_file):
        self.remove_file =remove_file
        try:
            self.connection=mysql.connector.connect(
                host=host,
                user=user,
                port=port,
                password=password
            )
            print(f'Connected to {host}')
        except Error as err:
            print(f"Error: {err}")
    
    """ Execute MySQL query """
    def execute_query(self, query):
        cursor = self.connection.cursor()
        
        try:
            cursor.execute(query)
            results=[list(row) for row in cursor.fetchall()]
            return results
        except Error as err:
            print(f"Error: {err}")
    
    """ Show current number of DBs """
    def show_db(self):
        query = "SHOW DATABASES";
        db_list = self.execute_query(query)
        db_df = pd.DataFrame(db_list, columns=['Database'])
        return db_df
    
    """ Query returns number of current schemas in database """
    def count_db(self):
        query = "SELECT COUNT(*) FROM information_schema.SCHEMATA"
        return self.execute_query(query)[0][0]

    """ Process input list of databases to be removed, and returns as df """
    def remove_list(self, remove_file):
        with open(f"{remove_file}", 'r') as file:
            file = file.readlines()
            file = [line.replace('\n', '').replace (' ','') for line in file]
            file = pd.DataFrame(file, columns=['Database'])
    
        return file
    
    """ Iterate over data frame of databases to be removed and remove them """
    def drop_databases(self):
        for row in self.remove_list(self.remove_file).itertuples(index=True):
            database = getattr(row, 'Database')
            query = f"DROP DATABASE {database};"
            print(query)
            self.execute_query(query)
            

    """ Command line arguments """
    @staticmethod
    def parse_args():
        """add command line arguments"""
        parser = argparse.ArgumentParser(description="Clean up plants-prod-1/2 database")
        required = parser.add_argument_group("required arguments")
        required.add_argument("--host", action="store", dest="host", required=True)
        required.add_argument("--port", action="store", dest="port", required=True)
        required.add_argument("--user", action="store", dest="user", required=True)
        required.add_argument("--password", action="store", dest="password", required=True)
        required.add_argument('--file', action="store", dest="remove_file", required=True)
        args = parser.parse_args()
        
        return args

def main():
    args = Database.parse_args()
    db = Database(args.host, args.user, args.port, args.password, args.remove_file)
    db.drop_databases()
    print(f"\nCurrent number of schemas in {args.host}: {db.count_db()}")
    
if __name__ == "__main__":
    main()

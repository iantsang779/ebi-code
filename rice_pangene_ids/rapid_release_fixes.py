import argparse
import mysql.connector
from mysql.connector import Error

class Database:
    """
        Initiate connection to MySQL database. Initialize self.cv_dict to get cultivar schema, use database argument to return schema name to self.cv
    """
    def __init__(self, host, user, port, database, password):
        self.database = database
        self.cv_dict = {
                "Azucena": "oryza_sativa_gca009830595v1_core_110_1",
                "Arc": "oryza_sativa_gca009831255v1_core_110_1",
                "Chaomeo": "oryza_sativa_gca009831315v1_core_110_1", 
                "Gobolsailbalam": "oryza_sativa_gca009831025v1_core_110_1", 
                "IR64": "oryza_sativa_gca009914875v1_core_110_1", 
                "Ketannangka": "oryza_sativa_gca009831275v1_core_110_1", 
                "Khaoyaiguang": "oryza_sativa_gca009831295v1_core_110_1", 
                "Larhamugad": "oryza_sativa_gca009831355v1_core_110_1",
                "Lima": "oryza_sativa_gca009829395v1_core_110_1", 
                "Liuxu": "oryza_sativa_gca009829375v1_core_110_1", 
                "Mh63": "oryza_sativa_gca001618785v1_core_110_1", 
                "N22": "oryza_sativa_gca001952365v2_core_110_1", 
                "Natelboro": "oryza_sativa_gca009831335v1_core_110_1", 
                "Nipponbare" : "oryza_sativa_gca001433935v1_core_110_1",
                "Nipponbare_otherfeatures": "oryza_sativa_gca001433935v1_otherfeatures_110_1",
                "Pr106": "oryza_sativa_gca009831045v1_core_110_1",
                "Zs97": "oryza_sativa_gca001618795v1_core_110_1"}
        self.cv = self.cv_dict[database]
        
        try:
            self.connection = mysql.connector.connect(
                host=host,
                user=user,
                port=port,
                database=self.cv,
                password=password
                )
            print(f"Connected to {self.cv}!") 
            
        except Error as err:
            print(f"Error: '{err}'")

    """
        Execute SQL queries, stores results in list
    """
    def execute_query(self, query):
        cursor = self.connection.cursor() 

        try:
            cursor.execute(query)
            results = [list(row) for row in cursor.fetchall()]
            return results
        except Error as err:
            print(f"Error: '{err}'")
    
    """
        Get GCA Accession number for each cultivar
    """
    def get_GCA(self):
        query = "SELECT meta_value FROM meta WHERE meta_key rlike 'assembly.accession';"
        GCA = self.execute_query(query)[0][0]
        return GCA

    """
        Get Display Name for each cultivar
    """
    def get_display_name(self):
        query = "SELECT meta_value FROM meta where meta_key rlike 'species.display_name';"
        display_name = self.execute_query(query)[0][0]
        full_display_name = f"{display_name} - {self.get_GCA()}"
        return full_display_name
    
    """
        Query for updating meta value with release version, production name, database and display name
    """
    def update_query(self):
        query = f"""UPDATE meta SET meta_value = '110' WHERE meta_key = 'schema_version';
UPDATE meta SET meta_value = '{self.cv[0:27]}' WHERE meta_key = 'species.production_name';
UPDATE meta SET meta_value = '{self.database}' WHERE meta_key = 'species.strain';
UPDATE meta SET meta_value = '{self.get_display_name()}' WHERE meta_key = 'species.display_name';
        """
        return query 

    """
        Query for altering table meta_key properties, and inserting patch version
    """
    def alter_query(self):
        query = """ALTER TABLE meta MODIFY meta_key varchar(64) NOT NULL;
ALTER TABLE meta MODIFY meta_value varchar(255) DEFAULT NULL;
INSERT INTO meta (species_id, meta_key, meta_value) VALUES (NULL, 'patch', 'patch_109_110_d.sql|Extend meta_key length to 64 - allow NULL in meta_value'); 
        """
        return query

    """
        Query for removing newer MySQL patches 
    """
    def delete_query(self):
        query = """DELETE FROM meta WHERE meta_key = 'species.strain_group';
DELETE FROM meta WHERE meta_value rlike 'patch_110_111';
DELETE FROM meta WHERE meta_value rlike 'patch_111_112_a';
DELETE FROM meta WHERE meta_value rlike 'patch_111_112_b';
        """
        return query

        
    @staticmethod  
    def parse_args():
        parser = argparse.ArgumentParser(description="Upload CV names")
        parser.add_argument("--host", help="Host", action="store", dest="host", required=True)
        parser.add_argument("-u", "--user", help="User", action="store", dest="user", required=True)
        parser.add_argument("-p", "--port", help="Port", action="store", dest="port", required=True)
        parser.add_argument("-d", "--database", help="Database", action="store", dest="database", required=True) 
        parser.add_argument("-pw", "--password", help="Password", action="store", dest="password")
        args = parser.parse_args()
        return args


def main():
    args = Database.parse_args()
    db = Database(args.host, args.user, args.port, args.database, args.password)
    print(db.update_query()) # generate the queries to be used in MySQL workbench
    print(db.alter_query())
    print(db.delete_query())
    
if __name__ == "__main__":
    main()
    
    

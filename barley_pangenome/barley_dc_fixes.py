import mysql.connector

class Database:
    
    def connect_to_db(self,database):
        try:
            connection=mysql.connector.connect(
                host='mysql-ens-plants-prod-1',
                user='ensro',
                port=4243,
                database=database
            )
            #print(f"Connected to {database}")
        except mysql.connector.Error as err:
            print(f"Error: {err}")
            
        return connection
    
    def execute_query(self, query, database):
        connection = self.connect_to_db(database)

        cursor = connection.cursor()
        
        try:
            cursor.execute(query)
            results = [list(row) for row in cursor.fetchall()]
            return results
        except mysql.connector.Error as err:
            print(f"Error: '{err}'")
    
    def meta_query(self):
        query = "SELECT meta_value FROM meta WHERE meta_key rlike 'species.production_name';"
        return query
    
    def update_meta_query(self, database):
        existing_value = self.execute_query(self.meta_query(), database)[0][0]
        new_value = f'prepl_{existing_value}'
        query = f"UPDATE meta SET meta_value = '{new_value}' WHERE meta_key = 'species.production_name';"
        
        return query
    
    def update_organism_name_query(self):
        query = "UPDATE meta SET meta_value = 'Hordeum vulgare' WHERE meta_key = 'organism.scientific_name';"
        return query
    
    def update_species_name_query(self):
        query = "UPDATE meta SET meta_value = 'Hordeum vulgare' WHERE meta_key = 'species.scientific_name';"
        return query
    
    def qc_biotype_query_gene(self):
        query = "SELECT biotype, COUNT(*) FROM gene GROUP BY biotype;"
        return query
    
    def qc_biotype_query_transcript(self):
        query = "SELECT biotype, COUNT(*) FROM transcript GROUP BY biotype;"
        return query 
    
    def check_protein_coding(self):
        query = "SELECT COUNT(*) FROM biotype WHERE name rLIKE 'protein_coding';"
        return query
        
    """
        Old db names with prepl_ prefix
    """
    def cultivar_list(self): 
        cvs = [
            'prepl_hordeum_vulgare_10tj18_core_60_113_1',
            'prepl_hordeum_vulgare_aizu6_core_60_113_1',
            'prepl_hordeum_vulgare_akashinriki_core_60_113_1',
            'prepl_hordeum_vulgare_barke_core_60_113_1',
            'prepl_hordeum_vulgare_bonus_core_60_113_1',
            'prepl_hordeum_vulgare_bowman_core_60_113_1',
            'prepl_hordeum_vulgare_chikurinibaraki1_core_60_113_1',
            'prepl_hordeum_vulgare_foma_core_60_113_1',
            'prepl_hordeum_vulgare_ft11_core_60_113_1',
            'prepl_hordeum_vulgare_ft144_core_60_113_1',
            'prepl_hordeum_vulgare_ft262_core_60_113_1',
            'prepl_hordeum_vulgare_ft286_core_60_113_1',
            'prepl_hordeum_vulgare_ft333_core_60_113_1',
            'prepl_hordeum_vulgare_ft628_core_60_113_1',
            'prepl_hordeum_vulgare_ft67_core_60_113_1',
            'prepl_hordeum_vulgare_ft880_core_60_113_1',
            'prepl_hordeum_vulgare_goldenmelon_core_60_113_1',
            'prepl_hordeum_vulgare_goldenpromise_core_60_113_1',
            'prepl_hordeum_vulgare_hid055_core_60_113_1',
            'prepl_hordeum_vulgare_hid101_core_60_113_1',
            'prepl_hordeum_vulgare_hid249_core_60_113_1',
            'prepl_hordeum_vulgare_hid357_core_60_113_1',
            'prepl_hordeum_vulgare_hid380_core_60_113_1',
            'prepl_hordeum_vulgare_hockett_core_60_113_1',
            'prepl_hordeum_vulgare_hor10096_core_60_113_1',
            'prepl_hordeum_vulgare_hor10350_core_60_113_1',
            'prepl_hordeum_vulgare_hor10892_core_60_113_1',
            'prepl_hordeum_vulgare_hor1168_core_60_113_1',
            'prepl_hordeum_vulgare_hor12184_core_60_113_1',
            'prepl_hordeum_vulgare_hor12541_core_60_113_1',
            'prepl_hordeum_vulgare_hor13594_core_60_113_1',
            'prepl_hordeum_vulgare_hor13663_core_60_113_1',
            'prepl_hordeum_vulgare_hor13821_core_60_113_1',
            'prepl_hordeum_vulgare_hor13942_core_60_113_1',
            'prepl_hordeum_vulgare_hor14061_core_60_113_1',
            'prepl_hordeum_vulgare_hor14121_core_60_113_1',
            'prepl_hordeum_vulgare_hor14273_core_60_113_1',
            'prepl_hordeum_vulgare_hor1702_core_60_113_1',
            'prepl_hordeum_vulgare_hor18321_core_60_113_1',
            'prepl_hordeum_vulgare_hor19184_core_60_113_1',
            'prepl_hordeum_vulgare_hor21256_core_60_113_1',
            'prepl_hordeum_vulgare_hor21322_core_60_113_1',
            'prepl_hordeum_vulgare_hor21595_core_60_113_1',
            'prepl_hordeum_vulgare_hor21599_core_60_113_1',
            'prepl_hordeum_vulgare_hor2180_core_60_113_1',
            'prepl_hordeum_vulgare_hor2779_core_60_113_1',
            'prepl_hordeum_vulgare_hor2830_core_60_113_1',
            'prepl_hordeum_vulgare_hor3081_core_60_113_1',
            'prepl_hordeum_vulgare_hor3365_core_60_113_1',
            'prepl_hordeum_vulgare_hor3474_core_60_113_1',
            'prepl_hordeum_vulgare_hor4224_core_60_113_1',
            'prepl_hordeum_vulgare_hor495_core_60_113_1',
            'prepl_hordeum_vulgare_hor6220_core_60_113_1',
            'prepl_hordeum_vulgare_hor7172_core_60_113_1',
            'prepl_hordeum_vulgare_hor7385_core_60_113_1',
            'prepl_hordeum_vulgare_hor7552_core_60_113_1',
            'prepl_hordeum_vulgare_hor8117_core_60_113_1',
            'prepl_hordeum_vulgare_hor8148_core_60_113_1',
            'prepl_hordeum_vulgare_hor9043_core_60_113_1',
            'prepl_hordeum_vulgare_hor9972_core_60_113_1',
            'prepl_hordeum_vulgare_igri_core_60_113_1',
            'prepl_hordeum_vulgare_maximus_core_60_113_1',
            'prepl_hordeum_vulgare_oun333_core_60_113_1',
            'prepl_hordeum_vulgare_rgtplanet_core_60_113_1',
            'prepl_hordeum_vulgare_wbdc078_core_60_113_1',
            'prepl_hordeum_vulgare_wbdc103_core_60_113_1',
            'prepl_hordeum_vulgare_wbdc133_core_60_113_1',
            'prepl_hordeum_vulgare_wbdc184_core_60_113_1',
            'prepl_hordeum_vulgare_wbdc199_core_60_113_1',
            'prepl_hordeum_vulgare_wbdc207_core_60_113_1',
            'prepl_hordeum_vulgare_wbdc237_core_60_113_1',
            'prepl_hordeum_vulgare_wbdc348_core_60_113_1',
            'prepl_hordeum_vulgare_wbdc349_core_60_113_1',
            'prepl_hordeum_vulgare_zdm01467_core_60_113_1',
            'prepl_hordeum_vulgare_zdm02064_core_60_113_1']

        return cvs
    """
        New db names with prepl_ prefix removed
    """
    def renamed_cultivar_list(self):
        old_cvs = self.cultivar_list()
        
        new_list = []
        for i in old_cvs:
            new_cvs = i[6:]
            new_list.append(new_cvs)
        
        return new_list
        
    """
        Update species prod name, organisim scientific name, species scientific name 
    """
    def update_cultivars(self):
            
        for i in self.renamed_cultivar_list():
            self.connect_to_db(i)
            print(self.update_meta_query(i))
            #self.execute_query(self.update_meta_query())
            print(self.update_organism_name_query())
            #self.execute_query(self.update_organism_name_query())
            print(self.update_species_name_query())
            #self.execute_query(self.update_species_name_query())
    
    """ 
        Check gene, transcript, and non-translating CDS for each cultivar in schema.biotype
    """
    def check_biotype(self):
        for i in self.renamed_cultivar_list():
            print(i)
            self.connect_to_db(i)
            
            gene = self.execute_query(self.qc_biotype_query_gene(), i)
            transcript = self.execute_query(self.qc_biotype_query_transcript(), i)
            print('Protein Coding: ', self.execute_query(self.check_protein_coding(), i))
            print('Gene: ', gene)
            print('Tran: ', transcript)
    
    """
        Prints queries to rename db from prepl_hord... to hordeum_vulg....
        Paste queries into .txt and execute on codon with 'rename_db mysql-ens-plants-prod-1-ensrw prepl_... hordeum_vulgare_...'
    """
    def rename_db(self):
        for i in self.cultivar_list():
            query = f'rename_db mysql-ens-plants-prod-1-ensrw {i} {i[6:]}'
            print(query)

def main():
    db = Database()
    #db.update_cultivars()
    db.check_biotype()
    #db.rename_db()
    #db.renamed_cultivar_list()
    
    
    
if __name__ == "__main__":
    main()
    

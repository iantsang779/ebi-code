import sys

c1_id = sys.argv[1] #cultivar 1 accession name
c1_GCA = sys.argv[2] #cultivar 1 accession number "GCA..."
c2_id = sys.argv[3]
c2_GCA = sys.argv[4]
c3_id = sys.argv[5]
c3_GCA = sys.argv[6]
c4_id = sys.argv[7]
c4_GCA = sys.argv[8]
c5_id = sys.argv[9]
c5_GCA = sys.argv[10]


sp_name = "Barley" #species common name
taxon_id = 4513 #taxon id


"""
PLEASE CHECK! NEED TO ADD _cv_ TO THE URLS FOR CULTIVARS, BUT NOT LANDRACES OR WILD!
"""

#URLs 
c1_GCA_URL = f"{c1_GCA[4:7]}/{c1_GCA[7:10]}/{c1_GCA[10:13]}/{c1_GCA}_Hvulgare_cv_{c1_id}_BPGv2/"
c2_GCA_URL = f"{c2_GCA[4:7]}/{c2_GCA[7:10]}/{c2_GCA[10:13]}/{c2_GCA}_Hvulgare_cv_{c2_id}_BPGv2/"
c3_GCA_URL = f"{c3_GCA[4:7]}/{c3_GCA[7:10]}/{c3_GCA[10:13]}/{c3_GCA}_Hvulgare_cv_{c3_id}_BPGv2/"
c4_GCA_URL = f"{c4_GCA[4:7]}/{c4_GCA[7:10]}/{c4_GCA[10:13]}/{c4_GCA}_Hvulgare_cv_{c4_id}_BPGv2/"
c5_GCA_URL = f"{c5_GCA[4:7]}/{c5_GCA[7:10]}/{c5_GCA[10:13]}/{c5_GCA}_Hvulgare_cv_{c5_id}_BPGv2/"

#Hardcoded paths - change as necessary
asm_url = "https://ftp.ncbi.nlm.nih.gov/genomes/all/GCA/"
fasta_dir = "/nfs/production/flicek/ensembl/plants/species/Barley/fasta/"
gff_dir = "/nfs/production/flicek/ensembl/plants/species/Barley/gff/"

with open ("_barley_list", "w") as out_file:
    out_file.write(f"_ABBREV_\t_PROD_NAME_\t_PEP_FASTA_PATH_\t_GFF_FASTA_PATH_\t_INSDC_ACCESSION_\t_ASM_URL_\t_ASM_REP_FILE_\t_FNA_FILE_\t_GBFF_FILE_\t_SP_COMMON_NAME_\t_TAXON_ID_\n")
    out_file.write(f"{c1_id.lower()}\thordeum_vulgare_{c1_id.lower()}\t{fasta_dir}{c1_id}.fa\t{gff_dir}{c1_id}.gff3\t{c1_GCA}\t{asm_url}{c1_GCA_URL}\t{c1_GCA}_Hvulgare_cv_{c1_id}_BPGv2_assembly_report.txt\t{c1_GCA}_Hvulgare_cv_{c1_id}_BPGv2_genomic.fna.gz\t{c1_GCA}_Hvulgare_cv_{c1_id}_BPGv2_genomic.gbff.gz\t{sp_name}\t{taxon_id}\n") 
    out_file.write(f"{c2_id.lower()}\thordeum_vulgare_{c2_id.lower()}\t{fasta_dir}{c2_id}.fa\t{gff_dir}{c2_id}.gff3\t{c2_GCA}\t{asm_url}{c2_GCA_URL}\t{c2_GCA}_Hvulgare_cv_{c2_id}_BPGv2_assembly_report.txt\t{c2_GCA}_Hvulgare_cv_{c2_id}_BPGv2_genomic.fna.gz\t{c2_GCA}_Hvulgare_cv_{c2_id}_BPGv2_genomic.gbff.gz\t{sp_name}\t{taxon_id}\n") 
    out_file.write(f"{c3_id.lower()}\thordeum_vulgare_{c3_id.lower()}\t{fasta_dir}{c3_id}.fa\t{gff_dir}{c3_id}.gff3\t{c3_GCA}\t{asm_url}{c3_GCA_URL}\t{c3_GCA}_Hvulgare_cv_{c3_id}_BPGv2_assembly_report.txt\t{c3_GCA}_Hvulgare_cv_{c3_id}_BPGv2_genomic.fna.gz\t{c3_GCA}_Hvulgare_cv_{c3_id}_BPGv2_genomic.gbff.gz\t{sp_name}\t{taxon_id}\n")
    out_file.write(f"{c4_id.lower()}\thordeum_vulgare_{c4_id.lower()}\t{fasta_dir}{c4_id}.fa\t{gff_dir}{c4_id}.gff3\t{c4_GCA}\t{asm_url}{c4_GCA_URL}\t{c4_GCA}_Hvulgare_cv_{c4_id}_BPGv2_assembly_report.txt\t{c4_GCA}_Hvulgare_cv_{c4_id}_BPGv2_genomic.fna.gz\t{c4_GCA}_Hvulgare_cv_{c4_id}_BPGv2_genomic.gbff.gz\t{sp_name}\t{taxon_id}\n")
    out_file.write(f"{c5_id.lower()}\thordeum_vulgare_{c5_id.lower()}\t{fasta_dir}{c5_id}.fa\t{gff_dir}{c5_id}.gff3\t{c5_GCA}\t{asm_url}{c5_GCA_URL}\t{c5_GCA}_Hvulgare_cv_{c5_id}_BPGv2_assembly_report.txt\t{c5_GCA}_Hvulgare_cv_{c5_id}_BPGv2_genomic.fna.gz\t{c5_GCA}_Hvulgare_cv_{c5_id}_BPGv2_genomic.gbff.gz\t{sp_name}\t{taxon_id}\n")

                       

### Transform rice pangene matrix ###

import pandas as pd
import re

with open('pangene_matrix_genes.tr.tab', 'r') as file:
    df = pd.read_csv(file, sep='\t') # read as tab seperated file
    df = df.replace('gene:', '', regex=True) # remove 'gene:' using regex
    df = df.melt(id_vars=['pangene_id'], var_name='cv_name', value_name='stable_id') # melt data frame and rename columns
    mask_empty = df['stable_id'] == '-'  # mask row where stable id = -
    mask_empty_2 = df['cv_name'] == 'Unnamed: 17'# mask row where cv_name is not correct
    df = df[~mask_empty]
    df = df[~mask_empty_2]
    df = df.sort_values(by=['pangene_id']) # sort by pangene_id
    df['stable_id'] = df['stable_id'].str.split(',') # split stable_id by ,
    df = df.explode('stable_id').reset_index(drop=True) # join the split stable_ids for each cell into a new row below and reset index

    df.to_csv('pangene_matrix_genes_transformed.tab', sep='\t')

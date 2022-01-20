import os
import pandas as pd
import csv

RAW_LOCAL_PATH = '../data/RAW/'
CURATED_LOCAL_PATH = '../data/CURATED/BY-POSTAL-CODE/'

TITLE_FILE_NAME = 'valeursfoncieres-2020.txt'

class DataLoader():
    """
    Manage the data loading
    """
    
    def ensure_split(self):
        '''
        Break raw data into many files
        '''

        if(os.path.exists(CURATED_LOCAL_PATH)):
            return

        ignore_files = 0
        treated_files = 0

        os.makedirs(CURATED_LOCAL_PATH)
        
        with open(RAW_LOCAL_PATH + TITLE_FILE_NAME, encoding='utf-8') as file_stream:    
            file_stream_reader = csv.DictReader(file_stream, delimiter='|')
            
            open_files_references = {}

            for row in file_stream_reader:

                if(row['Nature mutation'] != 'Vente' or row['Code postal'] == ''):
                    ignore_files += 1
                    continue

                title_type = row['Code postal']

                # Open a new file and write the header
                if title_type not in open_files_references:
                    output_file = open(CURATED_LOCAL_PATH + '{}.csv'.format(title_type), 'w', encoding='utf-8', newline='')
                    dictionary_writer = csv.DictWriter(output_file, fieldnames=file_stream_reader.fieldnames)
                    dictionary_writer.writeheader()
                    open_files_references[title_type] = output_file, dictionary_writer
                # Always write the row
                open_files_references[title_type][1].writerow(row)
                treated_files += 1

            # Close all the files
            for output_file, _ in open_files_references.values():
                output_file.close()

            print('Split {0} files into postal code dataset and ignore {1} files with missing code postal or wrong nature'.format(treated_files, ignore_files))

    def get_data(self, postal_code: str) -> pd.DataFrame:
        df = pd.read_csv('{0}{1}.csv'.format(CURATED_LOCAL_PATH, postal_code), decimal=',')

        df = df[df['Code type local'].notna()] # Remove missing local type
        df = df.loc[(df['Code type local'] != 3) & (df['Code type local'] != 4)]

        df = df.astype({ 'Code type local':int, 'Valeur fonciere': float})

        df['Date mutation'] = pd.to_datetime(df['Date mutation'], infer_datetime_format=True)

        return df
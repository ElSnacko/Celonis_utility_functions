
import pandas as pd
import os
from os import listdir
from os.path import isfile, join



def file_chunking(mypath, targettables,chunksize=5000000):
    """This is a function that takes in a directory and a list of tables that you want to chunk. 
    It then creates a folder for each table and then chunks the files into smaller pieces."""
    #determine if a variable is a list 
    lenoftables = {}
    onlyfiles = [f for f in listdir(mypath) if
                 isfile(join(mypath, f))]  # this grabs all the files in the directory that is given by mypath
    pathlist = {}
    for t in targettables:  # This loop compares the files to the elements of targettables and addes them to specific lists
        filelist = []
        if not os.path.exists(mypath + t):
            os.makedirs(mypath + t)
        for files in onlyfiles:
            if t in files:
                print(f'file:{files}')
                filelist.append(files)
        pathlist[t] = filelist
    for targ in pathlist:  # This loop opens the files in chunks, counts the sum of the specific chunked files, and then saves the chunks in a folder created near by
        totalrows = 0
        for csv in pathlist[targ]:
            rows = 0
            chunked = pd.read_csv(mypath + csv, chunksize=chunksize, iterator=True, header=None)
            for n, chunks in enumerate(chunked):
                rows += len(chunks.index)
                chunks.dropna().to_csv(mypath + targ + '/' + csv.replace('.csv', '_' + str(n) + '.csv'), index=False)
                print(f'{n} made of {csv}')
            # print(csv)
            # print(rows)
            totalrows += rows
            lenoftables[targ] = totalrows
    print(lenoftables)

def table_union(mypathdict):
    """This creates SQL code to union  the ingested flat files which were chunked
    sample inputs :
    mypath = {'cnsmr_accnt_tag':r'\Attachments\cnsmr_accnt_tag',
            'cnsmr_accnt_strtgy_log':r'\Attachments\cnsmr_accnt_strtgy_log'}"""
    onlyfiles = {}
    if isinstance(mypath, dict):
        for paths in mypath:
            onlyfiles[paths] = [f for f in listdir(mypath[paths]) if isfile(join(mypath[paths], f))]
        for filelist in onlyfiles:
            print()
            print(f'CREATE TABLE {filelist} as')
            for n, tables in enumerate(onlyfiles):
                print(f'''SELECT * FROM {tables.replace(".", "_").replace(" ", "_")}''')
                if n == len(onlyfiles) - 1:
                    print(';')
                else:
                    print('''UNION''')
    else:
        print('mypath input is not dict data type')


def column_rename(table_files,ref_path, err=True):
    """This is a method to automate the renaming of columns in the celonis back end.
     Commonly for POVs you will get flat files that commonly these dont have column names.
      What this function does is iterates through the table names and refernces the look up
      excel file for an ordered list of orignal column names
      ['cnsmr_accnt_pymnt_jrnl','cnsmr_tag.csv','crdtr.csv','crdtr_grp.csv','dcmnt_rqst.csv','dcmnt_tmplt.csv'
    ,'rslt_cd.csv','strtgy.csv','strtgy_stp.csv' ,'tag.csv','tag_typ.csv','wrkgrp.csv']"""
    ref_excel = pd.ExcelFile(ref_path)
    for i in table_files:
        try:
            for g,colname in enumerate(pd.read_excel(ref_excel,i)['Column_Name'].values):
                if str(colname) != 'nan':
                    print()
                    print(f'ALTER TABLE {i.replace(".", "_")}_csv')
                    print(f'''RENAME COLUMN "COL_{g}" TO "{colname.lower()}"''')
                    print(';')
        except ValueError:
            if err:
                print(f'--{i} was not in the excel')
            else:
                continue
import glob
import os

import click
from pyspark.sql import SparkSession
from pyteomics.mztab import MzTab


@click.command('mztab_to_parquet', short_help='')
@click.option('-i', help="Input mztab files. ie., /path/to/abc.mztab or /path/to/*", required=True)
@click.option('-o', help="Output path to store parquets. ie., /out/path", required=True)
def mztab_to_parquet(i, o):
     sql_context = SparkSession.builder.getOrCreate()

     files = glob.glob(i)
     for f in files:
         try:
             print("======= processing:" + f)
             mz_tab = MzTab(f, table_format='df')
             pep = mz_tab.peptide_table
             # print(pep)
             if not str(o).endswith('/'):
                 o = o + '/'
             out_file = o + os.path.basename(f).replace('.mzTab', '.snappy.parquet')
             print('writing: ' + out_file)
             pep.to_parquet(out_file, compression='snappy')
         except Exception as e:
             print("** Error while processing: " + f)
             print(e)


if __name__ == '__main__':
    mztab_to_parquet()
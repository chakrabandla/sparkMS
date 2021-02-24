import os
import sys
import click
from pyspark.sql import SparkSession, functions
from pyspark.sql.functions import explode, countDistinct
from sparkms.commons import Fields, psmtable


@click.command('protein_stats', short_help='')
@click.option('-psm', help="Input psm parquet files. ie., /path/to/", required=True)
@click.option('-tpsm', help="Input psm_table parquet files. ie., /path/to/", required=True)
@click.option('-pep', help="Input peptide parquet files. ie., /path/to/", required=True)
@click.option('-pro', help="Input protein parquet files. ie., /path/to/", required=True)
@click.option('-O', '--out-path', help="Output path to store parquets. ie., /out/path", required=True)
def protein_stats(psm, tpsm, pep, pro, out_path):
    if not os.path.isdir(out_path):
        print('The output_path specified does not exist: ' + out_path)
        sys.exit(1)

    sql_context = SparkSession.builder.getOrCreate()
    df_psm = sql_context.read.parquet(psm)
    df_tpsm = sql_context.read.parquet(tpsm)
    df_pep = sql_context.read.parquet(pep)
    df_pro = sql_context.read.parquet(pro)

    pep_per_project = df_pep.groupby(Fields.EXTERNAL_PROJECT_ACCESSION).agg({Fields.PEPTIDE_ACCESSION: 'count'})\
        .toDF(Fields.EXTERNAL_PROJECT_ACCESSION, 'total_peptides')

    dist_pep_per_project = df_pep.groupby(Fields.EXTERNAL_PROJECT_ACCESSION).agg(countDistinct(Fields.PEPTIDE_ACCESSION).alias('total_distinct_peptides'))

    psms_per_project = df_psm.groupby(Fields.EXTERNAL_PROJECT_ACCESSION).agg({Fields.USI: 'count'}).toDF(Fields.EXTERNAL_PROJECT_ACCESSION, 'total_psms')

    max_confidence_score = df_tpsm.groupby(psmtable.PROJECT_ACCESSION).agg({psmtable.ID_SCORE_VALUE: 'max'}).toDF(Fields.EXTERNAL_PROJECT_ACCESSION, 'confidence')

    stats = pep_per_project.join(dist_pep_per_project, [Fields.EXTERNAL_PROJECT_ACCESSION])\
        .join(psms_per_project, [Fields.EXTERNAL_PROJECT_ACCESSION])\
        .join(max_confidence_score, [Fields.EXTERNAL_PROJECT_ACCESSION])\
        .select(Fields.EXTERNAL_PROJECT_ACCESSION, 'total_peptides', 'total_distinct_peptides', 'total_psms', 'confidence')
    stats.show(truncate=False)


if __name__ == '__main__':
    protein_stats()
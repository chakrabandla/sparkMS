import click
from pyspark.sql import SparkSession, functions
from pyspark.sql.functions import explode

from sparkms.commons import Fields, psmtable


@click.command('psm', short_help='')
@click.option('-psm', help="Input psm parquet files. ie., /path/to/", required=True)
@click.option('-pep', help="Input peptide parquet files. ie., /path/to/", required=True)
def peptide_evidence_by_protein_accession(psm, pep):
    sql_context = SparkSession.builder.getOrCreate()
    df_psm = sql_context.read.parquet(psm)
    df_pep = sql_context.read.parquet(pep)

    df_pep_exploded = df_pep.select(Fields.PROTEIN_ACCESSION, explode(Fields.PSM_SPECTRUM_ACCESSIONS).alias("psm"))
    df_pep_select = df_pep_exploded.groupby('psm.usi').agg(functions.collect_set(Fields.PROTEIN_ACCESSION)).toDF(Fields.USI, Fields.PROTEIN_ACCESSION)
    df_pep_select.show(truncate=False)

    df_psm_exploded = df_psm.select(Fields.USI, explode(Fields.ADDITIONAL_ATTRIBUTES).alias(Fields.ADDITIONAL_ATTRIBUTES), Fields.ASSAY_ACCESSION, Fields.PEPTIDE_SEQUENCE, Fields.MODIFIED_PEPTIDE_SEQUENCE, Fields.CHARGE, Fields.PRECURSOR_MASS, Fields.IS_DECOY)
    df_psm_filtered = df_psm_exploded.filter("additionalAttributes.accession == 'MS:1002355'")

    df_join = df_psm_filtered.join(df_pep_select, df_psm_filtered.usi == df_pep_select.usi, how='left')\
        .select(df_psm_filtered.usi, Fields.ASSAY_ACCESSION, Fields.PEPTIDE_SEQUENCE, Fields.MODIFIED_PEPTIDE_SEQUENCE, Fields.PROTEIN_ACCESSION, 'additionalAttributes.name', 'additionalAttributes.value', Fields.CHARGE, Fields.PRECURSOR_MASS, Fields.IS_DECOY)\
        .toDF(psmtable.USI, psmtable.PX_PROJECT_ACCESSION, psmtable.PEPTIDE, psmtable.MODIFIED_PEPTIDE, psmtable.PROTEINS, psmtable.ID_SCORE_NAME, psmtable.ID_SCORE_VALUE, psmtable.CHARGE, psmtable.MASS, psmtable.IS_DECOY)
    df_join.show(truncate=False)


if __name__ == '__main__':
    peptide_evidence_by_protein_accession()
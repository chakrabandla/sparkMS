import click
from pyspark.sql import SparkSession, functions
from sparkms.commons import Fields


@click.command('peptide_evidence_by_protein_accession', short_help='Group all peptide evidences using protein accession')
@click.option('-I', '--input-path', help="Input parquet files. ie., /path/to/", required=True)
@click.option('-F', '--filter', help="filter condition. i.e., proteinAccession=='Q5XI78' ", required=False)
def peptide_evidence_by_protein_accession(input_path, filter):
    sql_context = SparkSession.builder.getOrCreate()
    df = sql_context.read.parquet(input_path)
    # print(df)
    result = df.groupby(Fields.PROTEIN_ACCESSION).agg(functions.collect_set(Fields.PEPTIDE_SEQUENCE))
    if filter is not None:
        result = result.filter(filter)
        # result.show(truncate=False)
        return result


if __name__ == '__main__':
    peptide_evidence_by_protein_accession()
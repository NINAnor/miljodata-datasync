import duckdb
import typer
from openpyxl.utils.cell import get_column_letter
from python_calamine import CalamineWorkbook

from ..settings import log
from .app import app


@app.command(help="Convert SNP excel sheet to parquet")
def snp_database_normalize(
    file: str = typer.Argument(help="path to the file"),
    sheet: str = typer.Argument(
        default="Sheet1", help="Name of the Excel Sheet to use"
    ),
    start_row: int = typer.Option(default=3, help="Number of lines to skip"),
) -> None:
    """
    Convert the excel spreadsheet containing SNP data used by genetists
    The convertion will cleanup the alleles and make sure they are numbers

    :param file: Path to the file to open
    :type file: str
    :param sheet: Name of the sheet containing the data
    :type sheet: str
    :param start_row: First row to process
    :type start_row: int
    """
    db = duckdb.connect()

    # The header has some columns with empty names, as they depend on the previous
    wb = CalamineWorkbook.from_path(file)
    header = wb.get_sheet_by_name(sheet).to_python(skip_empty_area=False)[0]
    fixed_header = []
    for i, v in enumerate(header):
        if not v:
            fixed_header.append(f"{header[i - 1]}_Allele2")
        else:
            fixed_header.append(f"{v}_Allele1" if i > 4 else v)

    table = db.sql(f"""install excel; load excel;
        select *
        from read_xlsx('{file}', range = 'A{start_row}:{get_column_letter(len(fixed_header))}', header=false, all_varchar = true)
        """).to_arrow_table()  # noqa: E501, S608

    rel = db.from_arrow(table.rename_columns(fixed_header))
    row_numbers = rel.select("""row_number() OVER () as row_number,
                     * rename ("Fluidigm#" as fluidigm,
                        "NINA Genlab id" as fish_id,
                        "Vdr#" as river_id,
                        "Pop id" as pop_id
                     )
                     """)

    unpivoted = row_numbers.query(
        virtual_table_name="analysis",
        sql_query="""
            unpivot analysis
            on columns(* exclude(
                'row_number',
                'fluidigm',
                'fish_id',
                'GUID',
                'pop_id',
                'river_id'
            ))
            into
                name alle
                value alle_value
        """,
    )

    grouped = unpivoted.query(
        virtual_table_name="alleles",
        sql_query=r"""
            from alleles
            select
                row_number,
                fluidigm,
                fish_id,
                GUID,
                pop_id,
                river_id,
                first(regexp_replace(alle, '_Allele\d', '')) as gene,
                first(alle_value order by alle) as allele1,
                last(alle_value order by alle) as allele2
            group by
                row_number,
                fluidigm,
                fish_id,
                GUID,
                pop_id,
                river_id,
                regexp_replace(alle, '_Allele\d', '')
        """,
    )

    fixed = grouped.query(
        virtual_table_name="genes",
        sql_query="""from genes
            select * replace (
                case
                    when allele1 = 'T' then 4
                    when allele1 = 'G' then 3
                    when allele1 = 'C' then 2
                    when allele1 = 'A' then 1
                    when allele1 in ('-', 'N') then 0
                    else try_cast(allele1 as int)
                end as allele1,
                case
                    when allele2 = 'T' then 4
                    when allele2 = 'G' then 3
                    when allele2 = 'C' then 2
                    when allele2 = 'A' then 1
                    when allele2 in ('-', 'N') then 0
                    else try_cast(allele2 as int)
                end as allele2
            )
            where alle1 is not null and allele2 is not null
            order by row_number
        """,
    )

    fixed.to_parquet("genes.parquet")
    # NOTE: it's necessary to materialize first,
    # duckdb cannot unpivot and pivot in the same query
    # NOTE: all the operations are lazy, so everything up to this point
    # will be executed as a single query
    fixed_genes = db.read_parquet("genes.parquet")  # noqa: F841
    log.debug(fixed_genes)

    db.sql(
        """
            pivot fixed_genes
            on gene
            using first(allele1) as Allele1, first(allele2) as Allele2
            group by row_number, fluidigm, fish_id, GUID, pop_id, river_id
            order by row_number
        """,
    ).to_parquet("pivoted.parquet")

    # TODO: use original columns order

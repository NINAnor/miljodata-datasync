import io
import pathlib

import duckdb
import typer


def snp_analysis_to_parquet(
    file: str = typer.Argument(help="Path to the csv file"),
) -> None:
    """
    Convert the csv to a parquet file

    :param file: Path to the file csv containing the resut of a SNP analysis
    :type file: str
    """
    db = duckdb.connect()

    table = None
    filepath = pathlib.Path(file)

    with filepath.open(encoding="utf-8") as f:
        table = db.read_csv(
            io.BytesIO(
                "\n".join(
                    f.read().replace("\r", "").split("\n\n")[2].split("\n")[2:]
                ).encode()
            ).read()
        )

    table.select("* rename (column00 as position, column01 as genlab_id)").query(
        virtual_table_name="sample_positions",
        sql_query="""
            unpivot sample_positions
            on * exclude (position, genlab_id)
        """,
    ).filter("genlab_id != 'NTC'").select("""
        position,
        genlab_id,
        name as gene,
        case
            when lower(value) in ('no call') then 0
            when value[1] = 'T' then 4
            when value[1] = 'G' then 3
            when value[1] = 'C' then 2
            when value[1] = 'A' then 1
            else try_cast(value[1] as int)
        end as alle1,
        case
            when lower(value) in ('no call') then 0
            when value[3] = 'T' then 4
            when value[3] = 'G' then 3
            when value[3] = 'C' then 2
            when value[3] = 'A' then 1
            else try_cast(value[3] as int)
        end as alle2
    """).write_parquet(str(filepath.with_suffix(".parquet")))

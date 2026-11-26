"""Source-specific API clients built on shared ingestion utilities."""


def prepare_sources(connection, schema="bronze"):
    """Initialize registered sources before dbt builds, without calling their APIs."""
    from data_platform.sources import eia930, nws

    for source in (eia930, nws):
        for dataset in source.DATASETS:
            connection.execute(source.source_table_sql(schema, dataset))

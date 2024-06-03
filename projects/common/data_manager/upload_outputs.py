from datetime import datetime

from prefect import task

from projects.common.data_manager.postgres_connection import export_table_to_postgres


@task(name="[UPLOAD] Upload Outputs")
def upload_outputs(dict_dataset, database, name_table, export_option, environment, query=None):
    """
    Export of data according to the configuration.

    Args:
        dict_dataset (dict) : Dictionary with the all dataset to export
        database (str) : Database to export data.
        name_table (str) : Name of the output table.
        export_option (str) : Option to export.
        environment (str) : Environment to export (Production - Staging - Dev).
        query (str) : Specific query to export (only useful for upsert)

    Returns:
        Dict (df): Dict of DataFrame
    """

    if database == "Postgres":
        for data_to_export in dict_dataset.keys():
            df = dict_dataset[data_to_export]
            df["LastRunPipeline"] = datetime.now().strftime("%Y/%m/%d %H:%M:%S")
            export_table_to_postgres(
                df=df,
                name_table=name_table,
                overwrite=export_option,
                environment=environment,
                query=query,
            )

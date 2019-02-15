import pandas_gbq


class BigQueryExporter:
    def __init__(self, credentials, project_id, table_name):
        self.credentials = credentials
        self.project_id = project_id
        self.table_name = table_name

    def export(self, df):
        pandas_gbq.to_gbq(
            dataframe=df,
            destination_table=self.table_name,
            project_id=self.project_id,
            if_exists='append')

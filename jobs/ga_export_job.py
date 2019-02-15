# GAExportJob

from jobs.base_job import BaseJob
import pandas as pd


class GAExportJob(BaseJob):
    def __init__(self, analytics, view_id, start_date, end_date, exporter):
        self.analytics = analytics
        self.view_id = view_id
        self.start_date = start_date
        self.end_date = end_date
        self.exporter = exporter

    def _start(self):
        pass

    def _export(self):
        response = self.analytics.reports().batchGet(
            body={
                'reportRequests': [
                    {
                        'viewId': self.view_id,
                        'dateRanges': [{'startDate': self.start_date, 'endDate': self.end_date}],
                        'metrics': [{'expression': 'ga:sessions'}],
                        'dimensions': [{'name': 'ga:source'}, {'name': 'ga:country'}]
                    }]
            }
        ).execute()
        df = self.format_response(response)
        print(df)
        self.exporter.export(df)

    def _print_response(self, response):
        """Parses and prints the Analytics Reporting API V4 response.

        Args:
          response: An Analytics Reporting API V4 response.
        """
        for report in response.get('reports', []):
            column_header = report.get('columnHeader', {})
            dimension_headers = column_header.get('dimensions', [])
            metric_headers = column_header.get('metricHeader', {}).get('metricHeaderEntries', [])

            for row in report.get('data', {}).get('rows', []):
                dimensions = row.get('dimensions', [])
                date_range_values = row.get('metrics', [])

                for header, dimension in zip(dimension_headers, dimensions):
                    print(header + ': ' + dimension)

                for i, values in enumerate(date_range_values):
                    print('Date range: ' + str(i))
                    for metric_header, value in zip(metric_headers, values.get('values')):
                        print(metric_header.get('name') + ': ' + value)

    def format_response(self, response):
        data_list = []
        # get report data
        for report in response.get('reports', []):
            # set column headers
            column_header = report.get('columnHeader', {})
            dimension_headers = column_header.get('dimensions', [])
            metric_headers = column_header.get('metricHeader', {}).get('metricHeaderEntries', [])
            rows = report.get('data', {}).get('rows', [])

            for row in rows:
                # create dict for each row
                row_dict = {}
                dimensions = row.get('dimensions', [])
                date_range_values = row.get('metrics', [])

                # fill dict with dimension header (key) and dimension value (value)
                for header, dimension in zip(dimension_headers, dimensions):
                    row_dict[header] = dimension

                # fill dict with metric header (key) and metric value (value)
                for i, values in enumerate(date_range_values):
                    for metric, value in zip(metric_headers, values.get('values')):
                        # set int as int, float a float
                        if ',' in value or '.' in value:
                            row_dict[metric.get('name')] = float(value)
                        else:
                            row_dict[metric.get('name')] = int(value)

                data_list.append(row_dict)

        df = pd.DataFrame(data_list)
        column_dict = { column: column.replace(':', '_') for column in df.columns}
        df = df.rename(index=str, columns=column_dict)

        return df

# GAExportJob

from jobs.base_job import BaseJob
import pandas as pd


class YahooReportJob(BaseJob):
    def __init__(self, exporter):
        self.exporter = exporter

    def _start(self):
        # Initialize the job
        # For example set the configuration parameters
        pass

    def _export(self):
        # Get yahoo report data
        result = [
            {
                "timestamp": "20190228",
                "title": "Some title here",
                "post": "some description here"
            },
            {
                "timestamp": "20190227",
                "title": "Some title here2",
                "post": "some description here2"
            }
        ]
        df = pd.DataFrame(result)
        print(df)
        self.exporter.export(df)

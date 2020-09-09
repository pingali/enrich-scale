import os
import sys
import json
import numpy as np
import pandas as pd
from enrichsdk import Compute, S3Mixin
from datetime import datetime
import logging

logger = logging.getLogger("app")

from enrichsdk.contrib.lib.transforms.query_executor import QueryExecutorBase

class MyQueryExecutorExample(QueryExecutorBase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = "QueryExecutorExample"
        self.testdata = {
            'data_root': os.path.join(os.environ['ENRICH_TEST'],
                                      self.name),
            'statedir': os.path.join(os.environ['ENRICH_TEST'],
                                     self.name, 'state'),
            'conf': {
                'args': {
                    "cleanup": False,
                    "force": True,
                    "names": "all",
                    "start": "2020-08-01",
                    "end": "2020-08-03",
                }
            },
            "data": {}
        }

    @classmethod
    def instantiable(cls):
        return True

    def get_spec(self):

        thisdir = os.path.dirname(__file__)
        complete_spec = [
            {
                "name": "roomdb",
                "cred": "roomdb",
                "queries": [ 
                    {
                        "name": "select_star",
                        "output": "%(data_root)s/shared/db/select_star/%(dt)s.tsv",
                        "sql": "%(transform_root)s/SQL/select_star.sql",
                        "params": {
                            "alpha": 22
                        }
                    }
                ]
            },
            {
                "enable": False,
                "name": "hive",
                "cred": "hiveserver",
                "queries": [ 
                    {
                        "name": "employees",
                        "output": "%(data_root)s/shared/db/employee/%(dt)s.tsv",
                        "sql": "%(transform_root)s/SQL/employees.hql",
                    }
                ]
            }
        ]

        return complete_spec

provider = MyQueryExecutorExample

import os
import sys
import json
import numpy as np
import pandas as pd
from enrichsdk import Compute, S3Mixin
from datetime import datetime
import logging

logger = logging.getLogger("app")

class MyQueryExecutor(Compute, S3Mixin):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = "QueryExecutor"
        self.description = "What this transform is for?"
        
        # This is specifying preconditions to running this
        # transform. For example, inputframe1 should exist and should
        # have been touched by transform1 and transform2. inputframe2
        # should be touched by transformname3.
        self.dependencies = {
            "inputframe1": ["transform1", "transform2"],
            "inputframe2": "transform3"
        }

        # Test data used to check the functionality of this module.
        # Put this data in ENRICH_DATA/_test/
        # _test/
        #     transform2/
        #          inputframe1.csv
        #     transform3/
        #          inputframe2.csv
        #
        self.testdata = {
            'data_root': os.path.join(os.environ['ENRICH_TEST'],
                                      self.name),
            'statedir': os.path.join(os.environ['ENRICH_TEST'],
                                     self.name, 'state'),
            'conf': {
                'args': {
                    'path': "%(data_root)s"
                }
            },
            'data': {
                'inputframe1': {
                    'filename': 'inputframe1.csv',
                    'transform': 'transform2',
                    'params': {
                        'sep': ',',
                    }
                },
                'inputframe2': {
                    'filename': 'inputframe2.csv',
                    'transform': 'transform3',
                    'params': {
                        'sep': ',',
                    }
                }
            }
        }

    def validate_input(self, what, state):
        """
        Validate the input passed to this module

        Use can use any validation function. Recommended:
        pandera: https://github.com/cosmicBboy/pandera

        """
        pass

    def call_hive(self, params):

        # Initialize
        cred = params.get('cred',None)
        hive.init(params)

    def daily_generator(self, start, end):
        """
        Generate resolution parameters 
        """
        

    def process(self, state):
        """
        Run the computation and update the state
        """
        logger.debug("{} - process".format(self.name),
                     extra=self.config.get_extra({
                         'transform': self.name
                     }))

        framemgr = self.config.get_dataframe('pandas')

        start = self.args['start']
        end = self.args['end']
        
        for spec in self.args['specs']:
            name = spec['name']
            executor = self.lookup(spec['executor'])
            generator = self.lookup(spec.get('generator', 'default_generator'))
            paramlist = generator(spec)
            for params in paramlist: 
                try: 
                    executor(spec, params)
                except:
                    # Log the status
                    pass
                
        ###########################################
        # => Return
        ###########################################
        return state

    def validate_results(self, what, state):
        """
        Check to make sure that the execution completed correctly
        """

        framemgr = self.config.get_dataframe('pandas')
        pass

provider = MyQueryExecutor

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

        # This is specifying outputs of this transform. This module
        # generates two output frames outputframe1 and outputframe2,
        # they have the specified columns. This list is used to
        # validate the outputs. The description is used for
        # automatically generating the documentation of this module.
        #
        self.outputs = {
            "outputframe1": {
                "col1": "description",
                "col2": "description"
            },
            "outputframe2": {
                "col1": "description",
                "col2": "description"
            }
        }

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

    def process(self, state):
        """
        Run the computation and update the state
        """
        logger.debug("{} - process".format(self.name),
                     extra=self.config.get_extra({
                         'transform': self.name
                     }))

        ###############################################
        # => Initialize
        ###############################################
        # Dataframe object. This will expose additional functions
        # missing in the underlying dataframe (e.g., pandas)
        framemgr = self.config.get_dataframe('pandas')

        # => Get the frame details
        frame1_detail = state.get_frame('inputframe1')
        frame2_detail = state.get_frame('inputframe2')

        # => Extract the dataframe. This is usually a pandas
        # dataframe.
        df1 = frame1_detail['df']
        df2 = frame1_detail['df']

        ###############################################
        # => Compute
        ###############################################
        # Do the computation. Generate the updated/new pandas
        # dataframe.
        outputdf1 = ...
        outputdf2 = ...

        ###############################################
        # => Update state
        ###############################################

        # Annotate the dataframe with all/some columns that have been
        # introduced. If the output frame is a new one derived from
        # input frames, the first gather information columns of the
        # input frame.  self.collapse_columns(frame1_detail) Otherwise
        # gather all the columns
        columns = {}
        for c in list(outputdf1.columns):
            columns[c] = {
                'touch': self.name, # Who is introducing this column
                'datatype': framemgr.get_generic_dtype(df, c), # What is its type
                'description': self.get_column_description('outputframe1', c) # text associated with this column
            }

        # => Gather the update parameters
        updated_detail = {
            'df': outputdf1,
            'transform': self.name,
            'frametype': 'pandas',
            'params': [
                {
                    'type': 'compute',
                    'columns': columns
                }
            ],
            'history': [
                # Add a log entry describing the change
                {
                    'transform': self.name,
                    'log': 'your description',
                }
            ]
        }

        # Update the state.
        state.update_frame('outputframe1', updated_detail, create=True)

        # Do the same thing for the second update dataframe

        ###########################################
        # => Return
        ###########################################
        return state

    def validate_results(self, what, state):
        """
        Check to make sure that the execution completed correctly
        """

        framemgr = self.config.get_dataframe('pandas')

        ####################################################
        # => Output Dataframe 1
        ####################################################
        name = 'outputframe1'
        if not state.reached_stage(name, self.name):
            raise Exception("Could not find new frame created for {}".format(name))

        detail = state.get_frame(name)
        df = detail['df']

        # => Make sure it is not empty
        assert framemgr.shape(df)[0] > 0

        cols = framemgr.columns(df)
        for c in ['col1', 'col2']:
            if c not in cols:
                logger.error("Missing column: {}".format(c),
                             extra=self.config.get_extra({
                                 'transform': self.name
                             }))
                raise Exception("Invalid output generated")

provider = MyQueryExecutor
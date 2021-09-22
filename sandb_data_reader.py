#!/usr/bin/python3

import csv
import json
from utils import add_to_dict_num, add_to_dict_list
from record_reader_classes import classificationRow

class sandbDataReader:

    def __init__(self):

        self.data_rows = []
        self.user_index = {}
        self.workflow_subject_index = {}

    def load_data(self, data_file_name, version = None, after_date = None):
        # TODO: Add a date filter to only process latest transcriptions
        if after_date is None:
            date_limit = '1/1/2021'
        else:
            date_limit = after_date

        self.data_file_name = data_file_name
        file_handle = open(data_file_name, 'r')
        csv_reader = csv.reader(file_handle)
        next(csv_reader) #Ignore headings

        for row in csv_reader:
            if not version is None:
                row_version = float(row[6]) #Field index 6 is the workflow version
                if isinstance(version, float):
                    if row_version != version:
                        continue
                else: #Should be some indexable type defining min and max version nos, inclusive
                    if row_version < version[0] or row_version > version[1]:
                        continue
            R = classificationRow()
            R.add_row(row)
            self.data_rows.append(R)
            row_id = len(self.data_rows)-1
            user = R.get_by_key('user_name')
            add_to_dict_list(self.user_index, user, row_id)
            task = R.get_by_key('workflow_name')
            subject_name = R.get_by_key('subject_name')

            if task not in self.workflow_subject_index:
                self.workflow_subject_index[task] = {}
            task_dict = self.workflow_subject_index[task]
            add_to_dict_list(task_dict, subject_name, row_id)


    def workflow_subject_iter(self, workflow, min_count=1,max_count=100):
        
        subject_index = self.workflow_subject_index[workflow]
        for k in sorted(subject_index.keys()):
            v = subject_index[k]
            if min_count <= len(v) <= max_count:
                for row_id in v:
                    yield row_id

    def get_row_by_id(self, row_id):

        return self.data_rows[row_id]


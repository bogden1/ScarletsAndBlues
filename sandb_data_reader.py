#!/usr/bin/python3

import csv
import json
from datetime import datetime
from utils import add_to_dict_num, add_to_dict_list
from record_reader_classes import classificationRow

class sandbDataReader:

    def __init__(self):

        self.data_rows = []
        self.user_index = {}
        self.workflow_subject_index = {}

    def load_data(self, data_file_name, version = None, start_date = None, end_date = None):
        if start_date:
            start_date = datetime.strptime(start_date, '%Y-%m-%d') #Will fault on bad format
        if end_date:
            end_date = datetime.strptime(end_date, '%Y-%m-%d') #Will fault on bad format

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
            if start_date or end_date:
                str_date = json.loads(row[10])['finished_at'] #Field index 10 is the metadata
                assert str_date[10] == 'T'
                date = datetime.strptime(str_date[:10], '%Y-%m-%d') #Will fault on bad format
                if start_date and date < start_date:
                    continue
                if end_date and date > end_date:
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


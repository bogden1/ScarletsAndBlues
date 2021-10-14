#!/usr/bin/python3

import csv
import json
from datetime import datetime
from utils import add_to_dict_num, add_to_dict_list
from record_reader_classes import classificationRow
from utils import report

class sandbDataReader:

    def __init__(self):

        self.data_rows = []
        self.user_index = {}
        self.workflow_subject_index = {}

    def load_data(self, data_file_name, version = None, start_date = None, end_date = None, subject_ids = None, classification_ids = None):
        if start_date:
            start_date = datetime.strptime(start_date, '%Y-%m-%d') #Will fault on bad format
        if end_date:
            end_date = datetime.strptime(end_date, '%Y-%m-%d') #Will fault on bad format

        self.data_file_name = data_file_name
        file_handle = open(data_file_name, 'r')
        csv_reader = csv.reader(file_handle)
        next(csv_reader) #Ignore headings

        def report_skip(field, *args, **kwargs):
            metadata = json.loads(row[12])[row[13]]
            if 'Name' in metadata: name = metadata['Name']
            elif 'name' in metadata: name = metadata['name']
            else: name = metadata
            if 'reason' in kwargs: reason = kwargs['reason']
            elif 'key' in kwargs: reason = json.loads(row[field])[kwargs['key']]
            else: reason = row[field]
            report(2, f'Skipped classification {row[0]} (subject {row[13]} ({name})) due to field {field} == {reason}', *args)

        for row in csv_reader:
            if classification_ids:
                if classification_ids[0] == 0:
                    if     int(row[0]) in classification_ids[1:]:
                        report_skip(0, '(classification_id (exclusion))')
                        continue
                else:
                    if not int(row[0]) in classification_ids:
                        report_skip(0, '(classification_id (inclusion))')
                        continue
            if subject_ids:
                if subject_ids[0] == 0:
                    if     int(row[13]) in subject_ids[1:]:
                        report_skip(13, '(subject_id (exclusion))')
                        continue
                else:
                    if not int(row[13]) in subject_ids:
                        report_skip(13, '(subject_id (inclusion))')
                        continue
            if not version is None:
                row_version = float(row[6]) #Field index 6 is the workflow version
                if isinstance(version, float):
                    if row_version != version:
                        report_skip(6, '(version)')
                        continue
                else: #Should be some indexable type defining min and max version nos, inclusive
                    if row_version < version[0] or row_version > version[1]:
                        report_skip(6, '(version)')
                        continue
            if start_date or end_date:
                str_date = json.loads(row[10])['finished_at'] #Field index 10 is the metadata
                assert str_date[10] == 'T'
                str_date = str_date[:10]
                date = datetime.strptime(str_date, '%Y-%m-%d') #Will fault on bad format
                if start_date and date < start_date:
                    report_skip(10, '(early finish)', reason = str_date)
                    continue
                if end_date and date > end_date:
                    report_skip(10, '(late finish)', reason = str_date)
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
        
        if not workflow in self.workflow_subject_index: return
        subject_index = self.workflow_subject_index[workflow]
        for k in sorted(subject_index.keys()):
            v = subject_index[k]
            if min_count <= len(v) <= max_count:
                for row_id in v:
                    yield row_id

    def get_row_by_id(self, row_id):

        return self.data_rows[row_id]


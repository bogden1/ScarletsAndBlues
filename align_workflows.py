#!/usr/bin/env python3

from record_reader_classes import classificationRow, classificationRecordSet, taskActions
from record_aligner_class import recordAligner
from annotation_comparer import annotationComparer
from sandb_data_reader import sandbDataReader
from local_align import local_align, all_alignment
from collections import OrderedDict
from multi_align import MultiAlign
from utils import add_to_dict_num, add_to_dict_list


if __name__ == '__main__':

    workflow = "People"

    #data_file_name = "scarlets-and-blues-classifications.csv"
    data_files = {"Meetings": "exports/meetings-classifications.csv",
                  "People": "exports/people-classifications.csv"}

    data_file_name = data_files[workflow]

    DR = sandbDataReader()
    DR.load_data(data_file_name)

    C = annotationComparer()

    C.add_taskactions('People',   'annotations', 'create',['T20','T7'])  #, 'close':'T7', 'add':['T1','T2','T10','T11']})
    C.add_taskactions('People',   'annotations', 'close','T7')  #, 'close':'T7', 'add':['T1','T2','T10','T11']})
    C.add_taskactions('People',   'annotations', 'add',['T1','T2','T10','T11'])
    C.add_taskactions('Meetings', 'annotations', 'create',['T0','T7','T25','T14'])
    C.add_taskactions('Meetings', 'annotations', 'close',['T55','T37','T15','T14'])
    C.add_taskactions('Meetings', 'annotations', 'add',['T21','T23','T24','T20'])
    C.add_taskactions('Meetings', 'annotations', 'add',['T9','T3'])
    C.add_taskactions('Meetings', 'annotations', 'add',['T22','T6','T13','T10'])

    from calc_confidence import probabilityTree, similarityComparator, equalsComparator, missingComparator, confidenceCalculator
    PT1 = probabilityTree(similarityComparator(), {1:0.6, 2:0.3, '*': 0.1})
    PT2 = probabilityTree(equalsComparator(), {1:0.9, 0: PT1})
    PT3 = probabilityTree(missingComparator(), {1:0.7, -1:0.3, 0: PT2})

    #print("Ex1:", PT3.get_probability("jones","jones"))
    #print("Ex2:", PT3.get_probability("jones","jonesy"))
    #print("Ex3:", PT3.get_probability("jones","jonesyyy"))

    def align_prev_subject():
        #print("index",C.annotation_key_index)
        C.do_annotation_alignment()
        rec_ids = C.get_alignment_mapping()
        print("Rec ids", rec_ids)
        for al in C.alignments_iter([rec_ids]):
            if len(al) == 0:
                continue
            CC = confidenceCalculator(PT2)
            if len(al[1]) == 0:
                continue
            for a in al[1]:
                CC.add_value(a.get_delimited())
            print([C.annotation_key_index[x[0]] for x in al[0]],"\t",al[0],"\t",[x.get_delimited() for x in al[1]],"\t", next(CC.conf_iter()))

    prev_subject = ""
    counter = 0
    for row_id in DR.workflow_subject_iter(workflow,3,4):
        row = DR.get_row_by_id(row_id)
        subject_name = row.get_by_key("subject_name")
        classification_id = row.get_by_key("classification_id")
        print(row.items.keys())
        if subject_name != prev_subject:
            print("New subject:", subject_name)
            if prev_subject != "":
                align_prev_subject()
            subject_keys = []
            C.clear()
        C.add_row(row)
        subject_keys.append(classification_id)
        prev_subject = subject_name


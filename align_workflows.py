#!/usr/bin/env python3

from record_reader_classes import classificationRow, classificationRecordSet, taskActions
from record_aligner_class import recordAligner
from annotation_comparer import annotationComparer
from sandb_data_reader import sandbDataReader
from local_align import local_align, all_alignment
from collections import OrderedDict
from multi_align import MultiAlign
from utils import add_to_dict_num, add_to_dict_list

def new_record(old_paths, new_paths):
    #Map RecordSet index to Record index
    old_recs = {p[0]: p[1] for p in old_paths}
    new_recs = {p[0]: p[1] for p in new_paths}

    common_recordsets = list(set(new_recs.keys()).intersection(old_recs.keys()))

    if len(common_recordsets) == 0: #No common RecordSets, must be new Record
        return True
    else: #There is at least one RecordSet in common
        #If the Record is the same as before in every common RecordSet, we are still on the same Record
        #If the Record is different from before in every common RecordSet, we are on a new Record.
        #No other condition should be possible, so assert for that.
        base_rs = common_recordsets.pop()
        if new_recs[base_rs] == old_recs[base_rs]:
            for rs in common_recordsets:
                assert(new_recs[base_rs] == old_recs[base_rs])
            return False
        else:
            for rs in common_recordsets:
                assert(new_recs[base_rs] != old_recs[base_rs])
            return True


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
        print("New subject:", prev_subject)
        C.do_annotation_alignment()
        rec_ids = C.get_alignment_mapping() #List of pairwise record comparisons sufficient to compare all records
        print("Rec ids", rec_ids)

        old_paths = {}
        output_record_number = 0 #Will be incremented by 1 before the first output
        for paths, classifications in C.alignments_iter([rec_ids], depth = 1):
            if len(classifications) == 0:
                continue

            if new_record(old_paths, paths):
                output_record_number += 1

            CC = confidenceCalculator(PT2)
            classification_strs = [x.get_delimited() for x in classifications]
            for c in classification_strs:
                CC.add_value(c)
            print(output_record_number,"\t",[C.annotation_key_index[x[0]] for x in paths],"\t",paths,"\t",classification_strs,"\t", next(CC.conf_iter()))

            old_paths = paths
        C.clear()

    subject_it = DR.workflow_subject_iter(workflow)
    first_row = DR.get_row_by_id(next(subject_it))
    print(first_row.items.keys())
    C.add_row(first_row)
    prev_subject = first_row.get_by_key("subject_name")
    for row_id in subject_it:
        row = DR.get_row_by_id(row_id)
        subject_name = row.get_by_key("subject_name")
        classification_id = row.get_by_key("classification_id")
        if subject_name != prev_subject:
            align_prev_subject()
        print(row.items.keys())
        C.add_row(row)
        prev_subject = subject_name
    align_prev_subject()

#!/usr/bin/env python3

from record_reader_classes import classificationWord, classificationRow, classificationRecordSet, taskActions
from record_aligner_class import recordAligner
from annotation_comparer import annotationComparer
from sandb_data_reader import sandbDataReader
from local_align import local_align, all_alignment
from collections import OrderedDict, defaultdict
from multi_align import MultiAlign
from utils import add_to_dict_num, add_to_dict_list, report
import sys
import os.path

#Begin callbacks -- might make sense to put these in their own file
def standard_minute(ann):
    assert ann['task'] == 'T14_mc'
    v = ann['value']
    number = v[0]
    title, resolution = v[3:].split(':')
    return [
      {'task': 'T14_mc_sm_f', 'value': number},
      {'task': 'T14_mc_sm_f', 'value': title.strip()},
      {'task': 'T14_mc_sm_f', 'value': ''},
      {'task': 'T14_mc_sm_f', 'value': resolution.strip()},
      {'task': 'T14_mc_sm_r', 'value': None} #Dummy task to close the record on/open a new record
    ]

def attendance_list(ann):
    assert ann['task'] == 'T3'
    if len(ann['value'].strip()) == 0: return []
    attenders = []
    for a in ann['value'].split('\n'):
      attenders.append({'task': 'T3_f', 'value': a.strip()})
      attenders.append({'task': 'T3_r', 'value': None})
    return attenders

def table_col_headings(ann):
    assert ann['task'] == 'T23'
    v = ann['value'].strip()
    if len(v) == 0: return []
    return [{'task': f'{ann["task"]}_f', 'value': x.strip()} for x in v.split(',')]

def table_rows(ann):
    assert ann['task'] == 'T20'
    v = ann['value'].strip()
    if len(v) == 0: return []
    fields = []
    for row in v.split('\n'):
      fields.extend([{'task': f'{ann["task"]}_f', 'value': x.strip()} for x in row.split(',')])
      fields.append({'task': f'{ann["task"]}_r', 'value': None})

    #Just to create some space between tables in the CSV file
    fields.append({'task': f'{ann["task"]}_r', 'value': None})
    fields.append({'task': f'{ann["task"]}_f', 'value': ''})
    fields.append({'task': f'{ann["task"]}_r', 'value': None})

    return fields
#End callbacks

#Determine if two lists of paths, in which all paths begin with a RecordSet
#index followed by a Record index, mark a change in Record index.
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


workflow = {
    "persons": "People",
    "minutes": "Meetings",
    "attendance": "Meetings",
    "tables": "Meetings",
}

def main(args):
    headings = {
        "persons":    ['Surname', 'First name(s)', 'Title', 'Position', 'Subject', 'Pages'],
        "minutes":    ['#', 'Title', 'Item', 'Resolution', 'Subject'],
        "attendance": ['Name'],
        "tables":     [],
    }
    workflow_version = {"Meetings": args.meetings_version,
                        "People": args.people_version}
    data_files = {"Meetings": f"{args.indir}/meetings-classifications.csv",
                  "People": f"{args.indir}/people-classifications.csv"}
    
    report.verbosity = args.verbose
    for sub_workflow in args.sub_workflows:
      report.context = sub_workflow
      print(f'\nProcessing sub-workflow {sub_workflow}')
      print(  f'------------------------{"-" * len(sub_workflow)}')
      data_file_name = data_files[workflow[sub_workflow]]

      DR = sandbDataReader()
      DR.load_data(data_file_name, workflow_version[workflow[sub_workflow]], args.start_date, args.end_date, args.subject_ids, args.classification_ids)

      C = annotationComparer()
      C.add_taskactions('persons',   'annotations', 'create',['T20','T7'])
      C.add_taskactions('persons',   'annotations', 'close','T7')
      C.add_taskactions('persons',   'annotations', 'add',['T1','T2','T10','T11','T26','T6'])
      C.add_taskactions('minutes', 'annotations', 'create',['T8','T37','T15','T55','T0','T14_mc_sm_r'])
      C.add_taskactions('minutes', 'annotations', 'close',['T55','T15','T14_mc_sm_r'])
      C.add_taskactions('minutes', 'annotations', 'add',['T22','T13','T5','T6','T10','T14_mc_sm_f'])
      C.add_taskactions('minutes', 'annotations', standard_minute, 'T14_mc')
      C.add_taskactions('attendance', 'annotations', 'create', ['T11', 'T9_mc', 'T3', 'T3_r'])
      C.add_taskactions('attendance', 'annotations', 'close', ['T14', 'T9_mc', 'T3', 'T3_r'])
      C.add_taskactions('attendance', 'annotations', 'add', ['T9_mc', 'T3_f'])
      C.add_taskactions('attendance', 'annotations', attendance_list, 'T3')
      C.add_taskactions('tables', 'annotations', 'create', ['T8', 'T37', 'T20', 'T20_r', 'T23', 'T24'])
      C.add_taskactions('tables', 'annotations', 'close', ['T37', 'T20', 'T20_r', 'T23', 'T24'])
      C.add_taskactions('tables', 'annotations', table_rows, 'T20')
      C.add_taskactions('tables', 'annotations', table_col_headings, 'T23')
      C.add_taskactions('tables', 'annotations', 'add', ['T21', 'T24', 'T20_f', 'T23_f'])

      from calc_confidence import probabilityTree, similarityComparator, equalsComparator, missingComparator, confidenceCalculator
      PT1 = probabilityTree(similarityComparator(), {1:0.6, 2:0.3, '*': 0.1})
      PT2 = probabilityTree(equalsComparator(), {1:0.9, 0: PT1})
      PT3 = probabilityTree(missingComparator(), {1:0.7, -1:0.3, 0: PT2})

      #print("Ex1:", PT3.get_probability("jones","jones"))
      #print("Ex2:", PT3.get_probability("jones","jonesy"))
      #print("Ex3:", PT3.get_probability("jones","jonesyyy"))

      def align_prev_subject():
          #print("index",C.annotation_key_index)
          print(f"New subject: {prev_subject} {prev_subject_ids}")
          C.do_annotation_alignment()
          rec_ids = C.get_alignment_mapping() #List of pairwise record comparisons sufficient to compare all records
          #print("Rec ids", rec_ids)

          old_paths = {}
          output_record_number = -1 #Will be incremented by 1 before first use
          output_records = defaultdict(list)
          unresolved = defaultdict(lambda: 0)
          for paths, classifications in C.alignments_iter([rec_ids], depth = 1):
              if len(classifications) == 0:
                  continue

              if new_record(old_paths, paths):
                  output_record_number += 1

              CC = confidenceCalculator(PT2)
              classification_strs = [x.get_delimited() for x in classifications]
              for c in classification_strs:
                  CC.add_value(c)
              suggestion, probability, decibans = next(CC.conf_iter())

              #Fields consisting only of a blank word will contain only the word delimiter. Replace with empty string
              classification_strs = ['' if x == classificationWord.delimiter else x for x in classification_strs]
              if suggestion == classificationWord.delimiter:
                  suggestion = ''

              report(1, output_record_number,"\t",[C.annotation_key_index[x[0]] for x in paths],"\t",paths,"\t",classification_strs,"\t", [suggestion, probability, decibans])
              if decibans > 5:
                  output_records[output_record_number].append(suggestion)
              else:
                  output_records[output_record_number].append(classification_strs)
                  unresolved[output_record_number] += 1

              old_paths = paths

          C.clear()
          return [[unresolved[number], prev_subject, number + 1, *fields] for number, fields in output_records.items()]

      workflow_output = []
      subject_it = DR.workflow_subject_iter(workflow[sub_workflow], args.classifications, 999, args.sample)
      row_id = next(subject_it, None)
      if not row_id is None:
          first_row = DR.get_row_by_id(row_id)
          #print(first_row.items.keys())
          C.add_row(first_row, sub_workflow)
          prev_subject = first_row.get_by_key("subject_name")
          prev_subject_ids = first_row.get_by_key("subject_ids")
          for row_id in subject_it:
              row = DR.get_row_by_id(row_id)
              subject_name = row.get_by_key("subject_name")
              if subject_name != prev_subject:
                  workflow_output.extend(align_prev_subject())
              #print(row.items.keys())
              C.add_row(row, sub_workflow)
              prev_subject = subject_name
              prev_subject_ids = row.get_by_key("subject_ids")
          workflow_output.extend(align_prev_subject())

      import csv
      #In the CSV file, I want the 'Unresolved' column to have empty cells where nothing was unresolved
      for x in workflow_output:
          if x[0] == 0:
              x[0] = None

      with open(f'{args.outdir}/{sub_workflow}.csv', 'w') as f:
          w = csv.writer(f)
          w.writerow(['Unresolved', 'Page', 'Record'] + headings[sub_workflow])
          w.writerows(workflow_output)


if __name__ == '__main__':
    print(*sys.argv)

    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('sub_workflows', nargs = '*', default = workflow.keys(), help = f'Specify sub-workflows from {", ".join(workflow.keys())}. Will do all of them by default.')
    parser.add_argument('--start-date', '-s', help = 'Specify a start date to take classifications from (inclusive). YYYY-MM-DD format.')
    parser.add_argument('--end-date', '-e', help = 'Specify an end date to take classifications to (inclusive). YYYY-MM-DD format.')
    parser.add_argument('--classifications', '-c', type = int, default = 1, help = 'Minimum number of classifications required for inclusion.')
    parser.add_argument('--meetings-version', '-m', type = float, default = 72.196, help = 'Version number of the Meetings workflow to use.')
    parser.add_argument('--people-version', '-p', type = float, default = 31.82, help = 'Version number of the People workflow to use.')
    parser.add_argument('--subject-ids', nargs = '*', type = int, default = [], help = f'List (Zooniverse) subject ids to include. Put 0 as the first id to exclude them instead.')
    parser.add_argument('--classification-ids', nargs = '*', type = int, default = [], help = f'List (Zooniverse) classification ids to include. Put 0 as the first id to exclude them instead.')
    parser.add_argument('--outdir', '-o', default = 'output', help = 'Directory to place output files in.')
    parser.add_argument('--indir', '-i', default = 'exports', help = 'Directory to read Zooniverse data exports from.')
    parser.add_argument('--verbose', '-v', type = int, nargs = '?', default = 0, const = 1, help = 'Print more information to stdout. Add a numerical arg to increase verbosity level.')
    parser.add_argument('--force', action = 'store_true', help = 'Do not check that output directory is empty.')
    parser.add_argument('--sample', type = int, default = 0, help = 'Specify a number of classifications to use for each subject. Classifications will be randomly selected from all classifications for the subject. Total classifications for the subject must still be within the range required for inclusion, so subjects with too few or too many classifications will still be excluded.')

    args = parser.parse_args()

    if not os.path.isdir(args.outdir):
        print(f'Output directory "{args.outdir}" does not exist, creating...')
        os.mkdir(args.outdir)
    if os.listdir(args.outdir) and not args.force:
        print(f'Output directory "{args.outdir}" is not empty. --force to override.', file=sys.stderr)
        sys.exit(1)

    main(args)

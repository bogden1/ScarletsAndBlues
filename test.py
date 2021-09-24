#!/usr/bin/env python3

from sandb_data_reader import sandbDataReader
from annotation_comparer import annotationComparer

DR = sandbDataReader()
DR.load_data('test.csv', 1.0)

AC = annotationComparer()
AC.add_taskactions('Test', 'annotations', 'create', 'T1')
AC.add_taskactions('Test', 'annotations', 'close', 'T1')
AC.add_taskactions('Test', 'annotations', 'add', 'T2')

def dump_subject():
  AC.do_annotation_alignment()
  rec_ids = AC.get_alignment_mapping()
  count = 1
  for paths, classifications in AC.alignments_iter([rec_ids], depth = 1):
    print(count, [AC.annotation_key_index[x[0]] for x in paths], paths, [c.get_delimited() for c in classifications])
    count += 1
  print()

subject_name = 'Subject1'
subject_it = DR.workflow_subject_iter('Test')
for row_id in subject_it:
  row = DR.get_row_by_id(row_id)
  s = row.get_by_key('subject_name')
  if s != subject_name:
    print(subject_name)
    dump_subject()
    subject_name = s
    AC.clear()
  AC.add_row(row, 'Test')

print(subject_name)
dump_subject()

#test.csv has been carefully arranged
#I'm not sure exactly what output I expect here. If I'm happy to accept strings that are only entered once then really I think I want to see:
#Subject1:
#  Record 1: abc
#  Record 2: def OR g
#Subject2:
#  Record 1: abc OR g
#  Record 2: def
#Subject3:
#  Record 1: abc
#  Record 2: def
#  Record 3: ghi

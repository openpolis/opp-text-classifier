#!/opt/local/bin/python

from optparse import OptionParser
import csv

import MySQLdb
import sys

def main():
  usage = 'usage: verify.py [--quiet] FILE.csv'
  
  parser = OptionParser(usage)
  parser.add_option("-q", "--quiet", dest="quiet",
                    help="do not print to stdout", action="store_true")

  (options, args) = parser.parse_args()
  
  if len(args) == 0:
      print "File needed. Usage: %s" % usage
      exit(1)
      
  file_in = args[0]
  
  # open database connection and setup a cursor
  db = MySQLdb.connect("localhost","root","","op_openparlamento" )
  cursor = db.cursor()
  
  # read first csv file
  csv_reader = None
  try:
    csv_reader = csv.reader(open(file_in, 'U'), delimiter=',')
  except IOError:
    print "It was impossible to open file %s" % file_in
    exit(1)
  except csv.Error:
      print "CSV error while reading %s" % file_in, sys.exc_info()[0]
      
      
  # counters
  exactness = 0.
  inexactness = 0.
  cnt = 0
  
  # main loop over rows
  for r in csv_reader:
    cnt += 1

    act_id = r[0]
    auto_tags = r[1:]
    print "id: %s" % act_id
    print "  automatic tags: %s" % ','.join(auto_tags)

    sql = \
    """select distinct t.tag_id from sf_tagging t, opp_tag_has_tt ttt, opp_teseott tt where t.taggable_model='OppAtto' and  t.tag_id=ttt.tag_id and ttt.teseott_id = tt.id and taggable_id=%s and tt.denominazione != %s;"""
    cursor.execute(sql, (act_id, "localita'"))
    rows = cursor.fetchall()
    db_tags = []
    for row in rows:
        db_tags.append("%s" % row[0])
    print "  db tags: %s" % ','.join(db_tags)

    # transform lists into sets
    auto_set = frozenset(auto_tags)
    db_set = frozenset(db_tags)
    
    # compute set intersection
    common = auto_set & db_set
    print "  common: %s" % len(common)

    
    auto_only = auto_set - db_set
    print "  auto_only: %s" % len(auto_only)

    db_only = db_set - auto_set
    print "  db only: %s" % len(db_only)

    # increment counters
    n_common = len(common)
    n_auto_tags = len(auto_tags)
    if n_common > 0:
      exactness += n_common / n_auto_tags

    n_db_only = len(db_only)
    n_db_tags = len(db_tags)
    if n_db_only > 0:
      inexactness += n_db_only / n_db_tags

    # print out indicators
    print "%% di Esattezza (n_common/n_auto_tags): %s" % (100.0*exactness/cnt)
    print "%% di Inesattezza (n_db_only/n_db_tags): %s" % (100.0*inexactness/cnt)
  
  
  
  # close cursor and disconnect from server
  cursor.close()
  db.close()
    

if __name__ == "__main__":
  main()

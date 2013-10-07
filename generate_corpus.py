#!/opt/local/bin/python
import argparse
import MySQLdb
import os, os.path
from HTMLParser import HTMLParser
import codecs
from nltk import RegexpTokenizer
from nltk.corpus import stopwords

__author__ = 'guglielmo'

"""
This script helps generating a Categorized corpus (like the Reuters NLTK corpus)
that can be used for machine learning algorithms to assign tags to Openparlamento'c acts.

See Chapter 6 of Natural Language Processing With Python
and Chapter 7 of Python Text Processing With NLTK 2.0 Cookbook

usage::

    python generate_corpus.py --help


examples::
    python generate_corpus.py --db=op_openparlamento --act-types=3,4,5,6 --delete --limit=5000 ../corpora/opp_interrogazioni training
    python generate_corpus.py --db=op_openparlamento --act-types=3,4,5,6 --delete --offset=5001 --limit=5000 ../corpora/opp_interrogazioni test
    pushd ../corpora/opp_interrogazioni/
    cat cats_training.txt cats_test.txt > cats.txt
    popd

    python generate_corpus.py --db=op_openparlamento --macro --act-types=2,3,4,5,6 --delete --limit=5000 ../corpora/opp_interrogazioni_macro/ training
    python generate_corpus.py --db=op_openparlamento --macro --act-types=2,3,4,5,6 --delete --limit=5000 --offset=5000 ../corpora/opp_interrogazioni_macro/ test
    pushd ../corpora/opp_interrogazioni_macro/
    cat cats_training.txt cats_test.txt > cats.txt
    popd

TODO: the acts and contents are directly fetched from a Mysql DB.
Accessing them through an API would decouple the script, avoiding the necessity
of having it run on Openparlamento's server.

"""

##
# HTML tag stripping through standard library's HTMLParser
##

class MLStripper(HTMLParser):
    def __init__(self):
        self.reset()
        self.fed = []
    def handle_data(self, d):
        self.fed.append(d)
    def get_data(self):
        return ''.join(self.fed)

def strip_tags(html):
    s = MLStripper()
    s.feed(html)
    return s.get_data()

##
# MySQL extraction functions
##


def get_acts(**kwargs):
    """
    Returns list of act_ids of given act types,
    sorted by presentation date.
    """
    db_conn = kwargs['db']
    limit = kwargs['limit']
    offset = kwargs['offset']
    act_types_ids = kwargs['act_types_ids']

    # sql is built this way because the ids list must be inserted
    # normally the placehoslders should be evaluated safely inside cursor.execute(sql, string parameters)
    sql = """
        select id as act_id
         from opp_atto
         where tipo_atto_id in (%s) order by data_pres
    """ % (act_types_ids, )

    cursor = db_conn.cursor(MySQLdb.cursors.DictCursor)

    if limit is not 0:
        if offset is not 0:
            sql += " limit {0}, {1}".format(offset, limit)
        else:
            sql += " limit {0}".format(limit)

    cursor.execute(sql)
    rows = cursor.fetchall()
    cursor.close()

    return [row['act_id'] for row in rows]


def get_tags(act_id, **kwargs):
    """
    Returns the list of tags associated with acts of the given act
    """
    db_conn = kwargs['db']

    cursor = db_conn.cursor(MySQLdb.cursors.DictCursor)
    sql = """
        select t.id as tag_id, t.triple_value as tag_name, t.triple_namespace as tag_namespace
         from sf_tag t, sf_tagging g
         where g.taggable_model='OppAtto'
          and t.triple_namespace not like '%%geo%%'
          and g.tag_id=t.id and g.taggable_id=%s
    """
    cursor.execute(sql, act_id)
    rows = cursor.fetchall()
    cursor.close()

    return [str(row['tag_id']) for row in rows]

def get_macro_tags(act_id, **kwargs):
    """
    Returns the list of unnique top-tags associated with acts of the given act
    """
    db_conn = kwargs['db']

    cursor = db_conn.cursor(MySQLdb.cursors.DictCursor)
    sql = """
        select distinct tt.teseott_id as tag_id, ttt.denominazione as tag_name
         from sf_tag t, sf_tagging g, opp_tag_has_tt tt, opp_teseott ttt
         where g.taggable_model='OppAtto' and
          g.tag_id=t.id and tt.tag_id=t.id and ttt.id=tt.teseott_id and
          g.taggable_id=%s;
    """
    cursor.execute(sql, act_id)
    rows = cursor.fetchall()
    cursor.close()

    return [str(row['tag_id']) for row in rows]

def get_documents_text(act_id, **kwargs):
    """
    Returns the concatenated, tag-stripped text of all documents related to act_id
    """
    db_conn = kwargs['db']

    italian_stops = set(stopwords.words('italian'))


    cursor = db_conn.cursor(MySQLdb.cursors.DictCursor)
    sql = """
        select d.testo
         from opp_documento as d
         where d.atto_id=%s
    """
    cursor.execute(sql, act_id)
    rows = cursor.fetchall()
    cursor.close()

    testo = u''
    for row in rows:
        # strip html tags from texts, if present
        testo += unicode(
            strip_tags(
                row['testo']
            )
        )

    # remove stopwords
    tokenizer = RegexpTokenizer("[\w]+")
    words = tokenizer.tokenize(testo)
    filtered_testo = " ".join([word for word in words if word.lower() not in italian_stops])

    return filtered_testo


##
# generating function
##


def generate(**kwargs):
    """
    Extract texts from acts' documents in db and produces files in the specified prefixed path
    Write a cats_PREFIX.txt categories file
    PATH
    |- PREFIX
    |  |- ID1
    |  |- ID2
    |  |- ...
    |- cats_PREFIX.txt
    """
    path = kwargs['path']
    prefix = kwargs['prefix']
    macro = kwargs['macro']

    prefixed_path = os.path.join(path, prefix)
    if not os.path.exists(prefixed_path):
        os.mkdir(prefixed_path)

    # delete all files under prefixed_path, if required
    if kwargs['delete']:
        for the_file in os.listdir(prefixed_path):
            file_path = os.path.join(prefixed_path, the_file)
            try:
                if os.path.isfile(file_path):
                    os.unlink(file_path)
            except Exception, e:
                print e

    # write tags on cats_prefix.txt file
    cat_file = os.path.join(path, "cats_{0}.txt".format(prefix))
    f = codecs.open(cat_file, "w", "utf-8")

    for c, act_id in enumerate(get_acts(**kwargs)):
        print "{0}) {1}".format(c, act_id)

        # extract tags ids list
        if macro:
            tags_ids_list = ",".join(get_macro_tags(act_id, **kwargs))
        else:
            tags_ids_list = ",".join(get_tags(act_id, **kwargs))

        # only writes acts tags in cats_* file if there are some
        if tags_ids_list:

            # extract all texts from documents' acts
            testo = get_documents_text(act_id, **kwargs)

            # write to files only if there is a testo
            if testo:
                # write act's tags in file
                f.write(u"{0}/{1},{2}\n".format(prefix, act_id, tags_ids_list))

                # build text file name
                text_file_path = os.path.join(prefixed_path, str(act_id))

                # open text file in append mode, append content to it, close the file
                tf = codecs.open(text_file_path, "a", "utf-8")
                tf.write(testo)
                tf.close()
    f.close()


##
# Main function, called when directly calling the script
##
def main():

    # setup command-line args parser
    parser = argparse.ArgumentParser()

    parser.add_argument("-q", "--quiet", dest="quiet",
                        help="do not print to stdout", action="store_true")

    parser.add_argument("--db", dest="db",
                        help="the openparlamento database to extract data from, defaults to op_openparlamento",
                        default='op_openparlamento')

    parser.add_argument("--act-types-ids", dest="act_types_ids",
                        help="a comma separated list of acts types ids",
                        default='1')

    parser.add_argument("--limit", dest="limit",
                        help="limit the number of acts analyzed (0 = no limit)", type=int,
                        default=0)

    parser.add_argument("--offset", dest="offset",
                        help="offset of acts analyzed (0 = no offset)", type=int,
                        default=0)

    parser.add_argument("--delete", dest="delete",
                        help="remove previously created file in path and prefix", action='store_true',
                        default=False)

    parser.add_argument("--macro", dest="macro",
                        help="indicates that macro (top) categories should be extracted", action='store_true',
                        default=False)

    parser.add_argument("path",
                        help="where produced files and categories will be written, an absolute path")

    parser.add_argument("prefix",
                        help="prefix used for building different sets (training, test, dev1, dev2)")

    args = parser.parse_args()


    # connect to RDBMS server
    db = MySQLdb.connect(
        host="localhost",
        user="root",
        passwd="",
        db=args.db,
        charset='utf8',
        use_unicode=True
    )

    # build kwargs list for functions
    kwargs = {
        'act_types_ids': args.act_types_ids,
        'path': args.path, 'prefix': args.prefix,
        'delete': args.delete,
        'macro': args.macro,
        'limit': args.limit, 'offset': args.offset,
        'db': db,
    }

    # call generating function
    generate(**kwargs)


    # disconnect from server
    db.close()




if __name__ == "__main__":
  main()

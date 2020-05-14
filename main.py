import sqlite3
from rptag import RobinsonPierpontTag

connection = sqlite3.connect('nestle1904.sqlite')
cursor = connection.cursor()
cursor.execute('''
DROP TABLE IF EXISTS nestle1904;
''')
cursor.execute('''
CREATE TABLE nestle1904 (
    id INTEGER PRIMARY KEY,
    reference TEXT,
    text TEXT,
    normalized TEXT,
    strongs TEXT,
    lemma TEXT,
    morph_pos TEXT,
    morph_tense TEXT,
    morph_voice TEXT,
    morph_mood TEXT,
    morph_case TEXT,
    morph_gender TEXT,
    morph_person TEXT,
    morph_number TEXT,
    morph_possessor_number TEXT,
    morph_extra TEXT
)
''')

INSERTION_BATCH_SIZE = 25000
insertion_done_counter = 0
insertions = 0
values_to_insert = []
def do_insert(force=False):
    global insertion_done_counter
    global insertions
    global values_to_insert
    if insertions >= INSERTION_BATCH_SIZE or force:
        insertion_done_counter += 1
        print("Inserting batch %d with %d records" % (insertion_done_counter, insertions))
        insert_sql = """
        INSERT INTO nestle1904 (
            reference,
            text,
            normalized,
            strongs,
            lemma,
            morph_pos,
            morph_tense,
            morph_voice,
            morph_mood,
            morph_case,
            morph_gender,
            morph_person,
            morph_number,
            morph_possessor_number,
            morph_extra
        ) VALUES
        """ + ",".join(values_to_insert)
        cursor.execute(insert_sql)
        values_to_insert = []
        insertions = 0
def insert_line(data):
    global insertions
    global values_to_insert
    sqlized_data = { k: '"'+v+'"' if v is not None else "NULL" for k,v in data.items()}
    values_to_insert.append("""(
        {reference}, {text}, {normalized}, {strongs}, {lemma},
        {psp}, {tense}, {voice}, {mood}, {case}, {gender},
        {person}, {number}, {possessor_number}, {extra}
    )""".format(**sqlized_data))
    insertions += 1
    do_insert()

def check_insertions_are_done():
    global insertions
    if insertions > 0:
        do_insert(force=True)

headers = None
with open("./Nestle1904.csv") as file:
    for l in file:
        line = l.decode("UTF-8").encode("utf-8")
        if not line:
            continue

        if headers is None:
            headers = line.split("\t")
            continue

        word = line.split("\t")
        morph_code = word[3] # i.e. form_code
        word_details = vars(RobinsonPierpontTag(morph_code))
        word_details["reference"] = word[0]
        word_details["text"] = word[1]
        word_details["normalized"] = word[6]
        word_details["lemma"] = word[5]
        word_details["strongs"] = word[4]

        insert_line(word_details)

check_insertions_are_done()

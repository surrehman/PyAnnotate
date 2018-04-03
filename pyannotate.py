# DB file = books.db
# table   = annotation
# to sort annotation table according to creation date: _id

# _id = 4294967860                                          <-- This is the gets the book
# select * from books where _id ==  4294967860 ;            <-- This gets all the marks from the annotation table

# select * from annotation where content_id == 4294967860 ; <-- This gets all annotations form the book

# marked text   = It is almost a truism to say that the pure mathematician is not interested in the truth of his statements, but only in their internal consistency.
# mark          = OEBPS/html/0486222543_13_ch3.html#point(/1/4/82/1:203)
# mark_end      = OEBPS/html/0486222543_13_ch3.html#point(/1/4/82/1:350)

#point(/1/4/80/3:362)	
#point(/1/4/80/3:441)

import bs4
from lxml import etree
import os, pdb, codecs, sqlite3, pprint, zipfile, textwrap, io, re, StringIO, traceback
from collections import namedtuple, defaultdict

import sys  
reload(sys)  
sys.setdefaultencoding('utf8')



# To print out non-ascii in console: set PYTHONIOENCODING=UTF-8
# or alternatively:
sys.stdout = codecs.getwriter('utf8')(sys.stdout)
sys.stderr = codecs.getwriter('utf8')(sys.stderr)


def get_book_info(db_file):
    conn_obj   = sqlite3.connect(db_file)
    
    # to avoid repetitive lookup for the mapping b/w content_id and the 
    # book name, create mapping
    table_name = "books"

    book_fields    = '_id, title, kana_author, file_path, author, file_name'
    book_info_obj  = namedtuple('book_info_obj', book_fields.lstrip('_id,')) # first is the _id, use the rest
    book_info_dict = dict()

    sql_cmd        = ''' SELECT %s from %s;''' %(book_fields, table_name)
    cursor         = conn_obj.execute(sql_cmd)

    for an_item in cursor:
        book_info_dict[an_item[0]] = book_info_obj(*an_item[1:]) # first is the _id, use the rest
    return book_info_dict

def get_all_annotations(db_file):
    book_info_dict = get_book_info(db_file)
    conn_obj   = sqlite3.connect(db_file)

    table_name = "annotation"
    field_name = "content_id, CAST(mark as TEXT), CAST(mark_end as TEXT), marked_text"
    sql_cmd    = ''' SELECT %s from %s;''' %(field_name, table_name)
    cursor     = conn_obj.execute(sql_cmd)

    annotation_obj  = namedtuple('annotation_obj', 'book_info_obj, mark, mark_end, marked_text')
    annotation_dict = defaultdict(list) # indexed by book_id
    
    for an_item in cursor:
        book_id  = an_item[0]
        mark     = an_item[1].rstrip('\x00')
        mark_end = an_item[2].rstrip('\x00')
        marked_text = an_item[3]
        my_annotation_obj = annotation_obj(book_info_dict[book_id], mark, mark_end, marked_text.replace('\n', ''))
        annotation_dict[book_id].extend([my_annotation_obj])
               
    return  annotation_dict

def get_xhtml(zip_file_path, internal_path):
    """
    Extract the contents of an file inside an epub. Also copy the contents
    to a file (for debugging). 
    """

    epub_name = os.path.splitext(os.path.basename(zip_file_path))[0].strip()
    
    target_dir = os.path.join('tmp', epub_name)
    if not os.path.isdir(target_dir) : os.makedirs(target_dir)

    extracted_data_file = os.path.join(target_dir, internal_path.replace('/', '_'))
    # read from prev. saved file (if possible)
    if os.path.isfile(extracted_data_file) :
       in_fp       = open(extracted_data_file, 'rb')
       markup_data = in_fp.read()
       in_fp.close()
    # no prev. extracted markup, extract + save in tmp + return markup
    else:
        archive  = zipfile.ZipFile(zip_file_path, 'r')
        xml_data = archive.open(internal_path)
        markup_data     = xml_data.read()
        out_fp   = open(extracted_data_file, mode= 'wb')
        out_fp.write(markup_data)
        out_fp.close()

    return markup_data

def get_node_by_hier_path(hier_path, a_soup):
    current_node = a_soup
    while len(hier_path) != 0:
        current_node = current_node.contents[hier_path[0]]
        hier_path.pop(0)
    return current_node

def underline(some_text, underliner='-', overline=False):
    if overline:
        return  len(some_text) * underliner + '\n' + some_text + '\n' + len(some_text) * underliner + '\n'
    else :
        return   some_text + '\n' + len(some_text) * underliner + '\n'

def extract_node(mark, mark_end, xhtml_data):
    
    def point_info (a_mark) :
        _point_tuple = a_mark.split('#')[1].replace('point', '').split(':')
        _hier_tuple  = _point_tuple[0].lstrip('(').strip().split('/')
        hier_tupe    = [int(a) for a in _hier_tuple if a != '']
        offset       = int(_point_tuple[1].rstrip(')'))
        return [hier_tupe, offset]
    
    out_stream = StringIO.StringIO("")
    soup = bs4.BeautifulSoup(xhtml_data, 'lxml')

    start_point_info, start_offset = point_info(mark)
    end_point_info  , end_offset   = point_info(mark_end)

    out_stream.write("start_point_info, start_offset : %s\n" %str((start_point_info, start_offset)))
    out_stream.write("end_point_info, end_offset : %s\n" %str((end_point_info, end_offset)))


    node_hier_start = [a-1 for a in start_point_info][2:-1]
    node_hier_end   = [a-1 for a in end_point_info][2:-1]

    out_stream.write("node_hier_start = %s " %(node_hier_start))
    out_stream.write("node_hier_end   = %s\n" %(node_hier_end))

    start_node = get_node_by_hier_path(node_hier_start, soup.body)
    end_node   = get_node_by_hier_path(node_hier_end  , soup.body)


    # XXX: we are here and attempting to determing the start/stop issues
    # first when the mark, mark_end belong to the same node, the second case
    # fixes the issues where start and end are on two differnt nodes
    if start_node == end_node:
        out_stream.write("start offset= %s\n" % start_offset)
        out_stream.write("end   offset= %s\n\n" % end_offset)
        out_stream.write(unicode(start_node))
        
    return out_stream



def get_annotation_texts(out_stream=sys.stdout):
    """top-level entry point"""
    base_dir        = r'J:/Sony_Reader/media/books/'
    db_file_path    = r'J:/Sony_Reader/database/books.db'
    annotation_dict = get_all_annotations(db_file_path) 
    book_info_dict  = get_book_info(db_file_path)      

    _regex = r'#point(.+)'
    regex  = re.compile(_regex)


    non_empty_book_ids = [an_id for an_id in book_info_dict.keys() if annotation_dict[an_id] != []]
    # XXX: do the full monty or just the first couple of books?
    for a_book_id in non_empty_book_ids:
        if a_book_id != 4294967700 : continue
        book_info_obj       = book_info_dict[a_book_id]
        annotations_in_book = annotation_dict[a_book_id]

        print '-I- Processing %s'  % (book_info_obj.title)

        out_stream.write('\n' + underline(book_info_obj.title, underliner = '*') + '\n')
        out_stream.write('Book ID: (*%s*)\n\n' % a_book_id)


        for index, annotation_obj in enumerate(annotations_in_book):
            out_stream.write(underline("\nAnchors"))
            out_stream.write('    * %s\n    * %s\n\n' %(annotation_obj.mark, annotation_obj.mark_end ) )
            # first the marked_text field from database
            out_stream.write(underline("Marked text"))
            out_stream.write('' + annotation_obj.marked_text.strip() + '\n\n')

            # Now the extacted stuff from the DOM (if not pdf)

            out_stream.write(underline("Extracted node"))
            if  book_info_obj.file_path.endswith('pdf'): 
                out_stream.write("Cannot extract from pdf" + '\n')
                continue
            try:

                individual_xml_path = annotation_obj.mark.split('#')[0].strip()
                epub_file_name      = book_info_obj.file_path
                epub_path           = os.path.join(r"J:/", epub_file_name)
                data = get_xhtml(epub_path, individual_xml_path)
                from_doc = extract_node(mark        = annotation_obj.mark,
                                        mark_end    = annotation_obj.mark_end,
                                        xhtml_data  = data)
                out_stream.write(from_doc.getvalue() + '\n')
            except:
                out_stream.write("Extraction error:\n\n")
                py_error = traceback.format_exc()
                traceback_msg = '\n'.join(['|  ' + a for a in py_error.split('\n')])
                out_stream.write(traceback_msg + '\n')

            if (index != len(annotations_in_book)-1):
                out_stream.write('\n------\n\n')

    
if __name__ == '__main__':
    #in_stream = open(r'tmp\OPS_main15.xml')
    #with codecs.open('test.txt', 'w', 'utf-8') as out_stream:
    #    extract_node(out_stream = out_stream,
    #                 mark = r'OPS_main15.xml#point(/1/4/2/2/4/14/1:0)',
    #                 mark_end = r'OPS_main15.xml#point(/1/4/2/2/4/14/1:355)',
    #                 in_stream = in_stream)
    #in_stream.close()
    with codecs.open('Annotations.rst', 'w', 'utf-8') as out_stream:
        get_annotation_texts(out_stream)

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

import bs4, chardet
from lxml import etree
import os, pdb, codecs, sqlite3, pprint, zipfile, textwrap, io, re, StringIO, traceback
from collections import namedtuple, defaultdict
from pdb import set_trace

import sys  
reload(sys)  
sys.setdefaultencoding('utf8')



# To print out non-ascii in console: set PYTHONIOENCODING=UTF-8
# or alternatively:
sys.stdout = codecs.getwriter('utf8')(sys.stdout)
sys.stderr = codecs.getwriter('utf8')(sys.stderr)

DEBUG_INFO = 1


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
        my_annotation_obj = annotation_obj(book_info_dict[book_id], mark, 
                                           mark_end, 
                                           marked_text.replace('\n', ''))
        annotation_dict[book_id].extend([my_annotation_obj])
               
    return  annotation_dict


level = 0

def traverse(a_tag, func, node_index, **kwargs):
    global level
    func(a_tag, node_index, **kwargs)
    level +=1    
    for index, a_child in enumerate(a_tag):
        if not isinstance(a_tag, (str, unicode)):
            traverse(a_child, func, index, **kwargs)
    level -=1        
        
def prune_dom(a_node, dummy_index):
    #nodes_to_prune = (bs4.element.ProcessingInstruction, bs4.element.Comment)
    nodes_to_prune = (bs4.element.ProcessingInstruction)
    if isinstance(a_node,  nodes_to_prune):
        a_node.extract()

_clean_tag_type = lambda x: x.replace('<class ', '').strip('>').strip('\'').replace('bs4.element.', '')

def print_dom(a_tag, index=0, out_stream=sys.stdout):
    global level
    _repr = unicode(a_tag).replace('\n', ' ')
    out_stream.write("%s  [%03d] %s (%s) %s\n" % \
    (level * ' ' , index, _clean_tag_type(str(type(a_tag))), a_tag.name, _repr))

def get_xhtml_path(zip_file_path, internal_path):

    epub_name = os.path.splitext(os.path.basename(zip_file_path))[0].strip()
    
    target_dir = os.path.join('tmp', epub_name)
    if not os.path.isdir(target_dir) : os.makedirs(target_dir)
    extracted_data_file = os.path.join(target_dir, internal_path.replace('/', '_'))
    return '``soup=get_soup(r"%s")``' % extracted_data_file

def extract_xhtml(epub_file_path, internal_path):
    """
    Extract the contents of an file inside an epub. Also copy the contents
    to a file (for debugging). 
    """

    epub_name = os.path.splitext(os.path.basename(epub_file_path))[0].strip()
    
    target_dir = os.path.join('tmp', epub_name)
    if not os.path.isdir(target_dir) : os.makedirs(target_dir)

    extracted_data_file = os.path.join(target_dir, internal_path.replace('/', '_'))
    # prev. saved file 
    if os.path.isfile(extracted_data_file) :
        pass   
    # no prev. extracted markup, extract + save in tmp + return markup
    else:
        archive  = zipfile.ZipFile(epub_file_path, 'r')
        xml_data = archive.open(internal_path)
        markup_data     = xml_data.read()
        out_fp   = open(extracted_data_file, mode= 'wb')
        out_fp.write(markup_data)
        out_fp.close()

    return extracted_data_file

def get_node_by_hier_path(hier_path, a_soup):
    current_node = a_soup
    while len(hier_path) != 0:
        current_node = current_node.contents[hier_path[0]]
        hier_path.pop(0)
    return current_node



def underline(some_text, underliner='-', overline=False):
    if overline:
        return  len(some_text) * underliner + '\n' + some_text + '\n' + \
        len(some_text) * underliner + '\n'
    else :
        return   some_text + '\n' + len(some_text) * underliner + '\n'


def get_point_info (a_mark) :
    _point_tuple = a_mark.split('#')[1].replace('point', '').split(':')
    _hier_tuple  = _point_tuple[0].lstrip('(').strip().split('/')
    hier_tupe    = [int(a) for a in _hier_tuple if a != '']
    offset       = int(_point_tuple[1].rstrip(')'))
    return [hier_tupe, offset]


def ignore_tag(an_element):
    return type(an_element) not in [bs4.element.ProcessingInstruction,
                                    bs4.element.XMLProcessingInstruction,
                                    bs4.element.Comment,
                                    bs4.element.Declaration]

SOUP_CACHE          = dict()
STRAINED_SOUP_CACHE = dict()

def get_soup(file_path, encoding='utf-8'):
    global SOUP_CACHE
    if SOUP_CACHE.get(file_path) is not None: soup = SOUP_CACHE[file_path] 
    else:
        with codecs.open(file_path, 'r', encoding) as fp:
            contents = xml_line_filter(fp)
            soup = bs4.BeautifulSoup(contents, 'lxml')
        traverse(soup.body, prune_dom, 0)
        SOUP_CACHE[file_path] = soup
    return soup

def xml_line_filter(fp, on=True):
    out_stream =  StringIO.StringIO("")
    if on:
        for a_line in fp:
            if a_line.strip().startswith('<!--') and a_line.strip().endswith('-->'):
                out_stream.write('\n')
            else :
                out_stream.write(a_line)
    else:
        out_stream.write(fp.read())

    return out_stream.getvalue()

def get_strained_soup(file_path, encoding='utf-8'):
    global STRAINED_SOUP_CACHE
    if STRAINED_SOUP_CACHE.get(file_path) is not None:
        strained_soup = STRAINED_SOUP_CACHE[file_path]
    else:
        my_strainer = bs4.SoupStrainer(string = ignore_tag)
        with codecs.open(file_path, 'r', encoding) as fp:
            contents = xml_line_filter(fp)
            strained_soup = bs4.BeautifulSoup(contents, 'lxml', parse_only = my_strainer)
        
        traverse(strained_soup.body, prune_dom, 0)
        STRAINED_SOUP_CACHE[file_path] = strained_soup

    return strained_soup

def extract_node(location_str, annotation_obj, book_info_obj, encoding='utf-8'):
    global level
    mark                = getattr(annotation_obj, location_str)
    individual_xml_path = mark.split('#')[0].strip()
    epub_file_name      = book_info_obj.file_path
    out_stream          = StringIO.StringIO("")
    epub_path           = os.path.join(r"J:/", epub_file_name)
    local_xhtml_path    = get_xhtml_path(epub_path, individual_xml_path)

    try:
        data_path           = extract_xhtml(epub_path, individual_xml_path)

    except:
        out_stream.write("\nExtraction error:\n\n")
        out_stream.write("::\n\n")
        py_error = traceback.format_exc()
        traceback_msg = '\n'.join(['   ' + a for a in py_error.split('\n')])
        out_stream.write(traceback_msg + '\n')
        return out_stream, None

    with codecs.open(data_path, 'r', 'utf-8') as fp: 
        data = fp.read()

    
    soup          = get_soup(data_path, encoding)
    strained_soup = get_strained_soup(data_path, encoding)

    # dump the soup for later debugging:
    soup_dump = "%s.soup" % data_path
    with codecs.open(soup_dump, 'w', 'utf-8') as fp:
        traverse(strained_soup.body , print_dom , 0, out_stream=fp)
        

    point_info , offset = get_point_info(mark)
    node_hier = [a-1 for a in point_info][2:]

    if (DEBUG_INFO):
        out_stream.write("\n")
        out_stream.write("  * point_info      : %s\n" % str(point_info))
        out_stream.write("  * offset          : %s\n" % str(offset))
        out_stream.write("  * node_hier       : %s\n" % str(node_hier))
        out_stream.write("  * XHTML path      : %s\n" %(local_xhtml_path))
        out_stream.write("  * Encoding        : %s\n" %(encoding))
        out_stream.write("  * extrac. code    : ``get_node_by_hier_path(%s, soup.body)``\n" %(node_hier))
        out_stream.write("  * Soup            : ``%s``\n" %(soup_dump))

    try:
        extracted_node = get_node_by_hier_path(node_hier, strained_soup.body)
        out_stream.write('\n' + unicode(extracted_node))
        return out_stream, extracted_node

    except:
        out_stream.write("\nExtraction error:\n\n")
        out_stream.write("::\n\n")
        py_error = traceback.format_exc()
        traceback_msg = '\n'.join(['   ' + a for a in py_error.split('\n')])
        out_stream.write(traceback_msg + '\n')
        return out_stream, None

        
    

def get_annotation_texts(out_stream=sys.stdout):
    """top-level entry point"""
    base_dir        = r'J:/Sony_Reader/media/books/'
    db_file_path    = r'J:/Sony_Reader/database/books.db'
    annotation_dict = get_all_annotations(db_file_path) 
    book_info_dict  = get_book_info(db_file_path)      

    _regex = r'#point(.+)'
    regex  = re.compile(_regex)


    non_empty_book_ids = [an_id for an_id in book_info_dict.keys() if 
                          annotation_dict[an_id] != []]
    # XXX: do the full monty or just the first couple of books?
    for a_book_id in non_empty_book_ids:
        #if a_book_id not in [4294968997, 4294967700, 4294969467] : continue
        if a_book_id not in [4294968997, 4294967700] : continue
        book_info_obj       = book_info_dict[a_book_id]
        annotations_in_book = annotation_dict[a_book_id]

        print '-I- Processing %s %s'  % (book_info_obj.title, a_book_id)

        out_stream.write('\n' + underline(book_info_obj.title, underliner = '*') + '\n')
        out_stream.write('Book ID: (*%s*)\n\n' % a_book_id)


        for index, annotation_obj in enumerate(annotations_in_book):
            if (DEBUG_INFO):
                out_stream.write(underline("\nAnchors"))
                out_stream.write('    * %s\n    * %s\n\n' %(annotation_obj.mark, 
                                                           annotation_obj.mark_end))
                # first the marked_text field from database
            out_stream.write(underline("Marked text"))
            out_stream.write('' + annotation_obj.marked_text.strip() + '\n\n')

            # Now the extacted stuff from the DOM (if not pdf)
            if  book_info_obj.file_path.endswith('pdf'): 
                continue


            out_stream.write(underline("Extracted node"))

            from_doc_n1 , start_node = extract_node('mark',
                                                 annotation_obj,
                                                 book_info_obj)
            from_doc_n2 , end_node = extract_node('mark_end',
                                                annotation_obj,
                                                book_info_obj)

            if (DEBUG_INFO):
                # mark
                out_stream.write(underline("Mark:", underliner = '.'))
                out_stream.write(from_doc_n1.getvalue() + '\n')
                # mark end
                out_stream.write('\n')
                out_stream.write(underline("Mark end:", underliner = '.'))
                out_stream.write('  ' + from_doc_n2.getvalue() + '\n\n')

            if start_node is not None and end_node is not None:
                _ , start_offset = get_point_info(annotation_obj.mark)
                _ , end_offset   = get_point_info(annotation_obj.mark_end)
                #out_stream.write(underline('Start, end'  , underliner='~'))
                #out_stream.write('\n*  %d, %d \n' % (start_offset, end_offset))
                #out_stream.write("*  Node types: ``%s``, ``%s``\n\n" % (str(type(start_node)), str(type(end_node)))  )

                if start_node == end_node :
                    if type(start_node) == bs4.element.NavigableString:
                        _data     = bytes(unicode(start_node))
                        byte_data = _data[start_offset:end_offset]
                        _data     = unicode(start_node)
                        data      = _data[start_offset:end_offset]
                        try:
                            if data != byte_data:
                                # same node, data != byte_data
                                msg = underline('SameNodes ``(%s)``: Using ``bytes()``, start,end locations for ``NavigableString``'  % \
                                _clean_tag_type(str(type(start_node))),  underliner='~')
                                out_stream.write(msg)
                                out_stream.write(byte_data + '\n\n')

                                #msg = underline('SameNodes ``(%s)``: Using start,end locations for ``NavigableString``'  %\
                                #_clean_tag_type(str(type(start_node))),underliner='~')
                                #out_stream.write(msg)
                                #out_stream.write('\n' + data + '\n')
                            else:
                                # same node, data == byte_data
                                out_stream.write(underline('SameNodes ``(%s)`` ``data == byte_data``' %\
                                _clean_tag_type(str(type(start_node))), underliner='~'))
                                out_stream.write('\n' + data + '\n')
                        except:
                            # this is some wierd UnicodeDecodeError that I have 
                            # not figured out yet
                            out_stream.write(underline('Unicode error, fallback``(%s)`` ``data ? byte_data``' %\
                            _clean_tag_type(str(type(start_node))), underliner='~'))
                            fallback = annotation_obj.marked_text.strip() 
                            out_stream.write('\n' + fallback + '\n')
                    else:
                        # same nodes, but not NavigableString
                        msg = 'Same nodes but not NavigableString: %s, %s, via ``start_node.get_text()[start_offset, end_offset]``' %(str(type(start_node)), str(type(end_node)))
                        out_stream.write(underline(msg, underliner='~'))
                        data = start_node.get_text()[start_offset, end_offset]
                        out_stream.write(data + '\n')
                else:
                    # Case when the marked text extends  over nodes
                    _start_node_text = bytes(unicode(start_node))
                    _end_node_text   = bytes(unicode(end_node))
                    start_node_text  = _start_node_text[start_offset:]
                    end_node_text    = _end_node_text  [:end_offset]
                    combined         = start_node_text + end_node_text
                    msg              =  "Different nodes (combined)"
                    out_stream.write(underline(msg , underliner='~'))
                    out_stream.write( '\n' + combined + '\n')


            if (index != len(annotations_in_book)-1):
                out_stream.write('\n------\n\n')




    
if __name__ == '__main__':
    with codecs.open('Annotations.rst', 'w', 'utf-8') as out_stream:
        get_annotation_texts(out_stream)

    

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

DEBUG_INFO = 0
DUMP_SOUP  = 0

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
            soup = bs4.BeautifulSoup(bytes(contents), 'lxml', from_encoding=encoding)
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
            strained_soup = bs4.BeautifulSoup(bytes(contents), 'lxml', parse_only = my_strainer, from_encoding=encoding)
        
        traverse(strained_soup.body, prune_dom, 0)
        STRAINED_SOUP_CACHE[file_path] = strained_soup

    return strained_soup

def extract_node(location_str, annotation_obj, book_info_obj, encoding='utf-8'):
    global level
    mark                = getattr(annotation_obj, location_str)
    individual_xml_path = mark.split('#')[0].strip()
    epub_file_name      = book_info_obj.file_path
    out_stream          = StringIO.StringIO("")
    epub_path           = os.path.join(r"L:/", epub_file_name)
    local_xhtml_path    = get_xhtml_path(epub_path, individual_xml_path)
    siblings            = list()

    try:
        data_path           = extract_xhtml(epub_path, individual_xml_path)

    except:
        out_stream.write("\nExtraction error:\n\n")
        out_stream.write("::\n\n")
        py_error = traceback.format_exc()
        traceback_msg = '\n'.join(['   ' + a for a in py_error.split('\n')])
        out_stream.write(traceback_msg + '\n')
        return out_stream, (None, [])


    with codecs.open(data_path, 'r', 'utf-8') as fp: 
        data = fp.read()

    
    soup          = get_soup(data_path, encoding)
    strained_soup = get_strained_soup(data_path, encoding)

    # dump the soup for later debugging:
    soup_dump = "%s.soup" % data_path
    if (DUMP_SOUP):
        with codecs.open(soup_dump, 'w', 'utf-8') as fp:
            traverse(strained_soup.body , print_dom , 0, out_stream=fp)
        

    point_info , offset = get_point_info(mark)
    node_hier = [a-1 for a in point_info][2:]

    print_debug("\n", out_stream)
    print_debug("  * point_info      : %s\n" % str(point_info), out_stream)
    print_debug("  * offset          : %s\n" % str(offset), out_stream)
    print_debug("  * node_hier       : %s\n" % str(node_hier), out_stream)
    print_debug("  * XHTML path      : %s\n" %(local_xhtml_path), out_stream)
    print_debug("  * Encoding        : %s\n" %(encoding), out_stream)
    print_debug("  * extrac. code    : ``get_node_by_hier_path(%s, soup.body)``\n" %(node_hier), out_stream)
    print_debug("  * Soup            : ``%s``\n" %(soup_dump), out_stream)

    try:
        extracted_node = get_node_by_hier_path(node_hier, strained_soup.body)
        print_debug("  * node (unicode)  : %s\n" %  unicode(extracted_node), out_stream)
        print_debug("  * type(node)      : ``%s``\n" % _clean_tag_type(str(type(extracted_node))), out_stream)
        for sibling_node in extracted_node.find_next_siblings():
            siblings.append(sibling_node)
        return out_stream, (extracted_node, siblings)

    except:
        out_stream.write("\nExtraction error:\n\n")
        out_stream.write("::\n\n")
        py_error = traceback.format_exc()
        traceback_msg = '\n'.join(['   ' + a for a in py_error.split('\n')])
        out_stream.write(traceback_msg + '\n')
        return out_stream, (None, [])




def print_debug(data, out_stream):
    if not DEBUG_INFO: return
    else: out_stream.write(data)
    


def get_annotation_texts(out_stream=sys.stdout):
    """top-level entry point"""
    base_dir        = r'L:/Sony_Reader/media/books/'
    db_file_path    = r'L:/Sony_Reader/database/books.db'
    annotation_dict = get_all_annotations(db_file_path) 
    book_info_dict  = get_book_info(db_file_path)      

    _regex = r'#point(.+)'
    regex  = re.compile(_regex)


    non_empty_book_ids = [an_id for an_id in book_info_dict.keys() if 
                          annotation_dict[an_id] != []]

    # sort the list of book_ids (for the time being according to number of 
    # annotations contained in the book
    def book_compare(id1, id2): 
        return cmp(len(annotation_dict[id2]), len(annotation_dict[id1]))
        
    non_empty_book_ids.sort(cmp=book_compare)
    out_stream.write('\n' + underline('Annotations', underliner = '#') + '\n')

    
    for a_book_id in non_empty_book_ids:
        book_info_obj       = book_info_dict[a_book_id]
        annotations_in_book = annotation_dict[a_book_id]

        print '-I- Processing %s %s'  % (book_info_obj.title, a_book_id)

        out_stream.write('\n' + underline(book_info_obj.title, underliner = '*') + '\n')
        out_stream.write('*%s*  (%s)\n\n' % (book_info_obj.author, a_book_id))
        
        for annotation_index, annotation_obj in enumerate(annotations_in_book):
            print_debug(underline("\nAnchors"), out_stream)
            print_debug('    * %s\n    * %s\n\n' %(annotation_obj.mark, 
                                                   annotation_obj.mark_end),
                                                       out_stream)
            # first the marked_text field from database
            print_debug(underline("Marked text"), out_stream)
            # final output (form database)
            #out_stream.write('* ' + annotation_obj.marked_text.strip() + '\n')
            out_string_from_db =  annotation_obj.marked_text


            print_debug(underline("\nExtracted node %d" % annotation_index), out_stream)

            # Now the extacted stuff from the DOM (if not pdf)
            if  book_info_obj.file_path.endswith('pdf'): 
                selection = out_string_from_db
                out_stream.write('* %s\n' % (selection))
                continue
            

            from_doc_n1 , (start_node, start_siblings) = extract_node('mark',
                                                    annotation_obj,
                                                    book_info_obj,
                                                    'utf-8')
            from_doc_n2 , (end_node, end_siblings) = extract_node('mark_end',
                                                  annotation_obj,
                                                  book_info_obj,
                                                  'utf-8')
            # mark
            print_debug(underline("Mark:", underliner = '.'), out_stream)
            print_debug(from_doc_n1.getvalue() + '\n', out_stream)
            # mark end
            print_debug('\n', out_stream)
            print_debug(underline("Mark end:", underliner = '.'), out_stream)
            print_debug('  ' + from_doc_n2.getvalue() + '\n\n', out_stream)

            if start_node is not None and end_node is not None:
                _ , start_offset = get_point_info(annotation_obj.mark)
                _ , end_offset   = get_point_info(annotation_obj.mark_end)

                if start_node == end_node :
                    if type(start_node) == bs4.element.NavigableString:
                        _data     = bytes(unicode(start_node))
                        byte_data = unicode(_data[start_offset:end_offset], errors='ignore')
                        _data     = unicode(start_node)
                        data      = _data[start_offset:end_offset]
                        
                        if data != byte_data:
                            # same node, data != byte_data
                            msg = underline('SameNodes ``(%s)``: Using ``bytes()``, start,end locations for ``NavigableString``'  % \
                            _clean_tag_type(str(type(start_node))),  underliner='~')
                            print_debug(msg, out_stream)
                            out_string = byte_data 

                        else:
                            # same node, data == byte_data
                            print_debug(underline('SameNodes ``(%s)`` ``data == byte_data``' %\
                            _clean_tag_type(str(type(start_node))),
                            underliner='~'), out_stream)
                            out_string = data
                                
                    else:
                        # same nodes, but not NavigableString
                        msg = 'Same nodes but not NavigableString: %s, %s, via ``start_node.get_text()[start_offset, end_offset]``' %(str(type(start_node)), str(type(end_node)))
                        print_debug(underline(msg, underliner='~'), out_stream)
                        data = start_node.get_text()[start_offset, end_offset]
                        out_string = data
                else:

                    #if (annotation_index == 8 ): import pdb; pdb.set_trace()
                    # Case when the marked text extends  over nodes/ subnodes
                    # start, end nodes are of differnt types: 

                    # if they share the same paret and are NavigableString
                    if start_node.parent == end_node.parent and \
                        isinstance(start_node, bs4.element.NavigableString) and \
                        isinstance(end_node  , bs4.element.NavigableString):
                        out_string = unicode(start_node.parent.text)
                        
                        #_start_node_text = bytes(unicode(start_node.parent.text))
                        #_end_node_text   = bytes(unicode(end_node))
                        #end_offset = _start_node_text.find(_end_node_text)
                        #out_string = _start_node_text[start_offset : end_offset ]
                        #out_string += _end_node_text
                        msg              =  "Different nodes, same parents"
                    # they do not share the same parent: combine
                    else:
                        _start_node_text = bytes(unicode(start_node[start_offset:]))
                        _end_node_text   = bytes(unicode(end_node[:end_offset]))
                        #out_string = _start_node_text[start_offset : end_offset  ]
                        out_string = _start_node_text + _end_node_text
                        msg              =  "Different nodes, diff parents"
                    print_debug(underline(msg , underliner='~'), out_stream)
                    #import pdb; pdb.set_trace()
                
                # finally the selection: which to put ? 
                from_epub     =  out_string.replace('\n', ' ').strip()
                from_database =  out_string_from_db.strip()
            # When either of the nodes are None
            else:
                from_epub = ''
                from_database =  out_string_from_db.strip()

            
            selection = from_database if (len(from_database) > len(from_epub)) else from_epub
            choice = 'SQL' if (len(from_database) > len(from_epub)) else 'EPUB'
            if selection.find('*') != -1 : selection= selection.replace('*', r'\*')
            out_stream.write('* %s\n' % (selection))

               
        
        #if a_book_id != non_empty_book_ids[-1]:
        #    out_stream.write('\n------\n\n')



    
if __name__ == '__main__':
    with codecs.open('Annotations.rst', 'w', 'utf-8') as out_stream:
        get_annotation_texts(out_stream)

    

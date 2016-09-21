import multiprocessing
import os
import operator

from multiprocessing_mapreduce import SimpleMapReduce


def get_file_size(filename):
    """Read a file and return a sequence of (word, occurances) values.
    """

    filename = filename

    print multiprocessing.current_process().name, 'reading', filename

    filesize = os.path.getsize(filename) >> 20
    """
    output format:
    [(word, 1), ...,(word, 1)]
    """
    return [(filename, filesize)]


def count_file_size(item):
    """Convert the partitioned data for a word to a
    tuple containing the word and the number of occurances.
    """
    filename, filesize = item

    return (filename, filesize)


if __name__ == '__main__':
    import glob

    input_files = glob.glob('/tmp/*.rst')

    mapper = SimpleMapReduce(get_file_size, count_file_size)
    all_filename_size_info = mapper(input_files)

    sorted_info = sorted(all_filename_size_info, key=operator.itemgetter(1))

    sorted_info.reverse()

    top_10 = sorted_info[:10]

    for filename, filesize in top_10:
        print '%s:        %d' % (filename, filesize)
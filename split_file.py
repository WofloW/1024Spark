import os
import fnmatch
import sys
import math



def split_file(data_dir, partition_num, input_file):
    """
    Generate split information
    :param partition_num:  bucket number
    :param input_file: single file: filename or multiple file filename_
    :return:
            split_info = {0:[(file_name0, start, end)], 1:[(file_name1, start, end)]}
            One split may has more than one file.
            split_info =  {0:[(file_name0, start, end), (file_name1, start, end)],
                           1:[(file_name1, start, end)]}

            file_info = [(file0_path, file0_size), (file1_path, file1_size)]
    """

    split_info = {}
    file_info = []
    # Single file
    if not input_file.endswith('_'):
        file_path = data_dir + '/' + input_file
        file_size = os.path.getsize(file_path)
        split_size = int(math.ceil(float(file_size) / partition_num))
        # Split file
        for i in range(partition_num):
            split_info[i] = []
            start = i * split_size
            if (start + split_size) > file_size:
                end = file_size
            else:
                end = start + split_size
            split_info[i].append((file_path, start, end))
        file_info = [(file_path, file_size)]
    # Multiple files
    else:
        # Get all file name by the base name
        # and calculate the total file size.
        # file_info = [[file_dir1, file_size], [file_dir2, file_size], ...]
        total_size = 0
        for root, dir_names, file_names in os.walk(data_dir):
            for file_name in fnmatch.filter(file_names, input_file + '*'):
                dir_file = root + '/' + file_name
                one_file_size = os.path.getsize(dir_file)
                total_size += one_file_size
                file_info.append((dir_file, one_file_size))

        # Get worker num(split num)
        split_size = int(math.ceil(float(total_size) / partition_num))

        # Split file
        start = 0
        used_file = 0
        for i in range(partition_num):
            remaining_size = split_size
            split_info[i] = []
            while remaining_size > 0:
                current_file_name = file_info[used_file][0]
                current_file_size = file_info[used_file][1]
                # Required remaining_size <= file remaining_size
                if remaining_size <= (current_file_size - start):
                    split_info[i].append((current_file_name, start, start + remaining_size))
                    if remaining_size == current_file_size - start:
                        start = 0
                        used_file += 1
                    else:
                        start = start + remaining_size
                    remaining_size = 0
                # Required remaining_size > file remaining_size
                else:
                    if used_file < len(file_info) - 1:
                        split_info[i].append((current_file_name, start, current_file_size))
                        remaining_size -= current_file_size - start
                        start = 0
                        used_file += 1

                    # This is the last file, then finish split
                    else:
                        split_info[i].append((current_file_name, start, current_file_size))
                        remaining_size = 0
    return split_info, file_info

# read the data from split and also keep unit ( i.e. get next line from next split)
# split_info : single file [(file_name, start, end)]
# or multiple files [(file_name0, start, end), (file_name1, start, end)]
# partition_id : id of this partition
# partition_num : how many reducers is in this task
# file_info : all files info in this task
# [(file0_path, file0_size), (file1_path, file1_size)]
def read_input(split_info, partition_id, partition_num, file_info):
    data = ""
    filename = ""
    start = 0
    read_size = 0

    # read data from the split_info for this mapper
    for file in split_info:
        filename = file[0]
        start = file[1]
        read_size = file[2] - file[1]
        data += read_data_from_file(filename, start, read_size)
    last_file_path = filename
    start = start + read_size

    # get the last filename of this mapper in file_info
    used_file = 0
    for file in file_info:
        if file[0] == last_file_path:
            break
        used_file += 1

    if used_file > len(file_info):
        raise Exception("can't find the last file in split")

    split_delimitter = '\n'

    # Remove the first split if mapper_id is not 0
    if partition_id != 0:
        if len(data.split(split_delimitter)) > 1:
            data = data.split(split_delimitter, 1)[1]
        else:
            data = ""
    # Get more split if the mapper is not the last mapper
    if partition_id != partition_num - 1:
        data += read_data_from_file(file_info[used_file][0], start, file_info[used_file][1] - start) \
            .split(split_delimitter, 1)[0]
    return data

 # read data from file
def read_data_from_file(filename, start, read_size):
    f = open(filename)
    f.seek(start)
    data = f.read(read_size)
    try:
        f.close()
    except:
        print "Error: can't close the original data file"
    return data

if __name__ == "__main__":
    # multiple input file
    curdir = "/Users/WofloW/USF/CS636/1024Spark/"
    dir = os.path.join(curdir, "example_data")
    # print dir
    print "------- multiple file reader --------"
    split_info, file_info = split_file(dir, 3, "functional_")
    print split_info
    print file_info
    text = read_input(split_info[1], 1, 3, file_info)
    print text

    # single input file
    print "\n\n------- single file reader --------"
    split_info, file_info = split_file(dir, 3, "functional.txt")
    print split_info
    print file_info
    text = read_input(split_info[2], 2, 3, file_info)
    print text
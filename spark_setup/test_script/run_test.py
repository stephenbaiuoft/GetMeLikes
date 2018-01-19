#!/usr/bin/

import re
import os
# python script for taking output.txt and renaming all files

with open("output.txt", "r") as fo:
    if fo is None:
        print("failed to open output.txt")
        exit(-1)
    cur = os.getcwd()
    rename_dir = cur+'/'+'renamed'
    if not os.path.exists(rename_dir):
        os.mkdir(rename_dir)

    for line in fo:
        if ".json.bz2" in line:
            # process this json.bz2 only
            rez = line.split('\n')
            old_file = rez[0]
            rez = old_file.split('/')
            # remove . and

            try:
                rez.remove('.')
            except Exception as ex:
                # do nothing if . is not there
                print("warning in parsing: ", ex)

            new_name = '_'.join(rez)
            try:
                print("try to rename to: ", new_name)
                os.rename(old_file, rename_dir + '/' + new_name)
            except Exception as ex:
                print("failed to rename: ", line)
                print(ex)

            #print(new_name)


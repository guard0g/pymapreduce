import MapReduce
import sys

MATRIXSIZE = 5

"""
Word Count Example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: document identifier
    # value: document contents
    key = record[0]

    if key == 'a':
        i = record[1]
        j = record[2]
        val = record[3]
        for k in range(MATRIXSIZE):
            mr.emit_intermediate((i,k),('a',j,val))
    else:
        j = record[1]
        k = record[2]
        val = record[3]
        for i in range(MATRIXSIZE):
            mr.emit_intermediate((i,k),('b',j,val))
            

def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    total = 0
    alist = {}
    blist = {}

    for item in list_of_values:
        if item[0]=="a":
            alist[item[1]]=item[2]
        else:
            blist[item[1]]=item[2]

    for i in range(MATRIXSIZE):
        if alist.has_key(i) and blist.has_key(i):
            total += alist[i] * blist[i]

    mr.emit((key[0],key[1],total))

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)

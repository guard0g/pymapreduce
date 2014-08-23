import MapReduce
import sys

"""
Word Count Example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line


def mapper(record):
    # key: document identifier
    # value: document content
    mr.emit_intermediate(record[0],record[1][:-10])


mylist = []
def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts    
    v = list_of_values[0]
    if v not in mylist:
        mylist.append(v)
        mr.emit(v)
            

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)

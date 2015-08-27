import MapReduce
import sys

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: document identifier
    # value: document contents
    key = record[0]
    value = record[1]
    words = value.split()
    for w in words:
        #print value,record
        mr.emit_intermediate(value,record)

def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    i = 1
    while i < len(list_of_values):
        mr.emit(list_of_values[0]+list_of_values[i])
        i+=1


# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)

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
      mr.emit_intermediate('stuff',record)

def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    #print type (list_of_values)
    i=0
    p=['','']
    while i < len(list_of_values):
        asym=False
        p[0],p[1] = list_of_values[i]
        j=0
        while j < len(list_of_values):
            if [p[1],p[0]]==list_of_values[j]:
                asym=False
                break
            else:
                asym=True
            j+=1
        if asym==True:
            mr.emit((p[1],p[0]))
            mr.emit((p[0],p[1]))
        i+=1



# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
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
    mr.emit_intermediate('stuff',record)

def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    c={}
    i=0
    p=['',0,0,0]
    q=['',0,0,0]
    while i < 21:
        p[0],p[1],p[2],p[3] = list_of_values[i]
        j=21
        while j < 43:
            q[0],q[1],q[2],q[3] = list_of_values[j]
            if p[2]==q[1]:
                if c.has_key((p[1],q[2])):
                    c[(p[1],q[2])]+=p[3]*q[3]
                else:
                    c[(p[1],q[2])]=p[3]*q[3]
            j+=1
        i+=1
    for k in c.keys():
        x,y=k
        z=(x,y,c[k])
        mr.emit(z)

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
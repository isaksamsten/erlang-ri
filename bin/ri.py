import csv, string, math, operator, itertools

class SemanticVector:
    def __init__(self, name, vector):
        self.name = name
        self.vector = vector

    def __getitem__(self, key):
        return self.vector[key]

    def has_index(self, i):
        return self.vector.has_key(i)

    def __iter__(self):
        return self.vector.iteritems()

def cosine(x, y):
    dot = dot_product(x, y)
    if dot == 0:
        return 0
    else:
        xm = magnitude(x)
        if xm == 0:
            return 0
        else:
            ym = magnitude(y)
            return dot / (xm * ym) if ym > 0 else 0

def dot_product(x, y):
    dot = 0
    for index, value in x:
        if y.has_index(index):
            dot += value * y[index]
    return dot

def magnitude(x):
    m = 0
    for index, value in x:
        m += value * value
    return math.sqrt(m)

def parse_semantic_vector(description):
    r = {}
    for item in description:
        if item.strip() == "empty":
            return dict()
        try:
            (index, value) = string.split(item, sep=":", maxsplit=1)
            r[int(index.strip())] = int(value.strip())
        except:
            print item
    return r

def words(v, startswith=""):
    for word in v.keys():
        if word.startswith(startswith):
            print word

def similarity(a, b, v, measure=cosine):
    return measure(v[a], v[b])
    
def most_similar(word, vectors, num=5, measure=cosine):
    pivot = vectors[word]
    for w, v in vectors.iteritems():
        yield w, cosine(pivot, v)
        

def load(name):
    vectors = {}
    with open(name) as f:
        reader = csv.reader(f, delimiter=",", quotechar="\"")
        for row in reader:
            name = row[0]
            vector = parse_semantic_vector(row[1:])
            vectors[name] = SemanticVector(name, vector)

    return vectors

if __name__ == "__main__":
    import code, sys
    print "*** Starting RI-inspector ***"
    code.interact(local=dict(ri = sys.modules[__name__]))

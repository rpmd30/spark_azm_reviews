import json
import gzip

def parse(filename):
  f = gzip.open(filename, 'r')
#   f = open(filename, 'r')
  entry = {}
  for l in f:
    l = l.strip().decode('utf-8')
    print(l)
    colonPos = l.find(':')
    if colonPos == -1:
      yield entry
      entry = {}
      continue
    eName = l[:colonPos]
    rest = l[colonPos+2:]
    entry[eName] = rest
  yield entry

for e in parse("./raw_data/Arts.txt.gz"):
  print(json.dumps(e))
  print(len(e['product/productId']))
  break
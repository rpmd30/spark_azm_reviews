import json
import gzip

def parse(filename):
#   f = gzip.open(filename, 'rb')
  f = open(filename, 'r')
  entry = {}
  for l in f:
    l = l.strip()
    colonPos = l.find(':')
    if colonPos == -1:
      yield entry
      entry = {}
      continue
    eName = l[:colonPos]
    rest = l[colonPos+2:]
    entry[eName] = rest
  yield entry

for e in parse("all.txt"):
  print(json.dumps(e))
  break
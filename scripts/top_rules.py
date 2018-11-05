import csv

import sys

with open(sys.argv[1], newline='') as csvfile:
    reader = csv.reader(csvfile, delimiter='\t', quoting=csv.QUOTE_NONE)
    lines = [l for l in reader]
    lines.sort(key=lambda l: (float(l[len(l) - 2]), float(l[len(l) - 1])), reverse=True)
    print(lines[:10])

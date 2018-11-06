import csv

import sys

with open(sys.argv[1], newline='') as csvfile:
    reader = csv.DictReader(csvfile, delimiter='\t', quoting=csv.QUOTE_NONE)
    total = 0
    more_than_1m = 0
    for row in reader:
        total += 1
        if int(row['property instances']) > 1_000_000:
            more_than_1m += 1

    print(more_than_1m / total)

import json
import sys

# The following code is purely illustrative
try:
    with open(sys.argv[1]) as votes_in:
        for line in votes_in:
            print(json.loads(line))
            break
except FileNotFoundError:
    print("Please download the dataset using 'poetry run exercise fetch-data'")

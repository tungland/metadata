import glob
import concurrent.futures


files = glob.glob("/home/larsm/basex/nasjonalbiblio/shards3/*")
results = []

def last_5(stri):
    return stri[-10:]

with concurrent.futures.ProcessPoolExecutor() as executor:
    for file in files:
        results.append(executor.submit(last_5, file))

for f in results:
    print(f.result())
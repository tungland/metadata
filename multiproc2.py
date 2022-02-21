import multiprocessing as mp
from time import sleep

tasks = [1, 2, 3, 4]
def func(var) :
    print(f"Start process {var}")
    sleep(var)
    print(f"End process {var}")

num_workers = mp.cpu_count()  

pool = mp.Pool(num_workers)
for task in tasks:
    pool.apply_async(func, args = (task,))

pool.close()
pool.join()
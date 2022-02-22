import multiprocessing as mp
from time import sleep

tasks = [1, 2, 3, 4]
name = "Lars"
def func(var1, var2) :
    print(f"Start process {var1}, {var2}")
    sleep(var1)
    print(f"End process {var1}, {var2}")

num_workers = mp.cpu_count()  

pool = mp.Pool(num_workers)
for task in tasks:
    pool.apply_async(func, args = (task, name))

pool.close()
pool.join()
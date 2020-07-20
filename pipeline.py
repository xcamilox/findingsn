import psycopg2
import time
from datetime import datetime

from frastro.frastro.core.data.archive.lasair_archive_cp import LasairArchive
from frastro.frastro.core.data.archive.tns_archive_cp import TNSServices
import multiprocessing
import threading
import concurrent.futures
import random

#calc am magnituds imports
import math
from astropy import units as u
from astropy.coordinates import Distance
import numpy as np
import pandas as pd
import random

def generateParrallelTask(data_list,processToCall,thredToCall):
    now = datetime.now()
    start_time = time.time()
    current_time = now.strftime("%d/%m/%Y, %H:%M:%S")
    number_parallel_process = int(multiprocessing.cpu_count() / 2)
    process_data_size = int(len(data_list) / number_parallel_process)
    print("---------------------------------------")

    print("{0}: Split process for {1} in {2} ".format(str(processToCall),len(data_list),process_data_size))

    data = np.array(data_list)
    jobs = []
    for i in range(number_parallel_process):
        # process = [.submit(self.setAllPhotoz, data[i:i + step - 1, :].tolist()) for i in range(0, data.shape[0], step)]
        process = multiprocessing.Process(target=processToCall,
                                          args=(data_list[i:i + process_data_size - 1, :].tolist(),thredToCall,))
        jobs.append(process)
        process.start()
    for j in jobs:
        j.join()

def processToSplit(data_list,thredToCall):
    id = random.random()
    now = datetime.now()
    start_time = time.time()
    current_time = now.strftime("%d/%m/%Y, %H:%M:%S")
    print("---------------------------------------")
    print("{0}: set Jobs {1}".format(str(thredToCall), id))

    size = 2
    step = int(len(data_list) / size)

    data = np.array(data_list)

    with concurrent.futures.ThreadPoolExecutor(max_workers=size) as executor:
        list_of_dfs = [executor.submit(thredToCall, data[i:i + step - 1, :].tolist()) for i in
                       range(0, data.shape[0], step)]

    now = datetime.now()
    current_time = now.strftime("%d/%m/%Y, %H:%M:%S")
    print("{0}: Complete all Threads for job {1} in{2}min".format(current_time, id,
                                                                  str((time.time() - start_time) / 60)[0:4]))




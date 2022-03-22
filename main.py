import json
import os
from multiprocessing import * 
from time import perf_counter
from bitcoin import main
import shutil
from math import ceil

def LoadSettings() -> dict:
    with open("settings.json", "r") as file:
        return json.loads(file.read())

def CropFile(file_name: str, folder: str, size: int) -> None:
    
    with open(file_name, "r") as file:
        files_num = 0
        lines = 1
        file_t = open(f"{folder}/temp_{files_num}.txt", "w")

        while True:
            line = file.readline()
            if not line:
                break

            file_t.write(line)

            lines += 1
            if lines % size == 0:
                file_t.close()
                files_num += 1
                file_t = open(f"{folder}/temp_{files_num}.txt", "w")

def comparison(folder: str,folder_last: str):

    list_dir = os.listdir(folder) 
    list_dir_l = os.listdir(folder_last)
    result_file = open(f"{folder}/result.txt", "w")

    for i in list_dir:
        data = open(f"{folder}/{i}", "r").read().split("\n")
        for m in list_dir_l:
            data2 = open(f"{folder_last}/{m}", "r").read().split("\n")
            d = data + data2

            if len(d) - len(set(d)) != 0:
                res = list(set(data) & set(data2))
                for r in res:
                    r = r.replace("\n","")
                    if r != "":
                        result_file.write(f"{r}\n")

    result_file.close()

def get_count_line(file_name):
    size_line = 36
    size_f = os.path.getsize(file_name)

    return int(size_f / size_line)

if __name__ == "__main__":
    
    freeze_support()
    
    timer = perf_counter()
    settings = LoadSettings()
    
    first_file = settings["FirstFile"]
    last_file = settings["LastFile"]
    out_file = settings["OutFile"]
    count_line = settings["CropFileSizeLine"]
    thr_count = settings["ThreadCount"]
    clear = settings["Clean"]

    print("[Preset]")
    print(f"File 1: {first_file}")
    print(f"File 2: {last_file}")
    print(f"Result file: {out_file}")
    print()
    print(f"Count line: {count_line}")
    print(f"Thread count: {thr_count}")
    print(f"Clear after work: {clear}")
    print()
    folder_first = first_file.replace(".txt", "") 
    folder_last = last_file.replace(".txt", "") 

    lst_thr = []
    time_path_lst = []

    folder_time = 0
    
    if not os.path.exists(folder_first):
        os.mkdir(folder_first)
    
    if not os.path.exists(folder_last):
        os.mkdir(folder_last)

    print("[Crop file]")
    p1 = Process(target=CropFile, args=(settings["FirstFile"], folder_first, count_line))
    p1.start()
    
    p2 = Process(target=CropFile, args=(settings["LastFile"], folder_last, count_line))
    p2.start()

    p1.join()
    p2.join()

    files_lst_first = os.listdir(folder_first)
    files_lst_last = os.listdir(folder_last)
    
    count_files = len(files_lst_first)
    count_files_on_folder = int(ceil(count_files / thr_count))
    
    for i in range(thr_count):
        if not os.path.exists(f"{folder_first}/{i}"):
            os.mkdir(f"{folder_first}/{i}")

    for i in range(count_files):
        if len(os.listdir(f"{folder_first}/{folder_time}/")) == count_files_on_folder:
            folder_time += 1

        shutil.move(f"{folder_first}/temp_{i}.txt", f"{folder_first}/{folder_time}/temp_{i}.txt")

    print(f"End crop [{round(perf_counter() - timer, 2)}s]")
    print()
    print("[Start analyze]") 
    for i in os.listdir(folder_first):
        path = f"{folder_first}/{i}"
        if len(os.listdir(path)) != 0:
            lst_thr.append(Process(target=comparison, args=(path, folder_last)))
            time_path_lst.append(path)
            lst_thr[-1].start()

    for i in lst_thr:
        i.join()

    print(f"End analyze [{round(perf_counter() - timer,2)}s]")
    print()
    print("[Write result]")
    result = open(out_file, "w")

    for x in time_path_lst:
        with open(f"{x}/result.txt", "r") as file:
            result.write(file.read())

    if clear:
        shutil.rmtree(f"{folder_first}", ignore_errors=True)
        shutil.rmtree(f"{folder_last}", ignore_errors=True)
    
    print("Done")
    print(f"Total time: {round(perf_counter() - timer,2)}s")
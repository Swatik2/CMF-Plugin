import json
import logging
import os
import glob
import warnings
from pathlib import Path
warnings.filterwarnings("ignore")

def getmlmdlist(file_path):
    filelist=[]
    filelist=rec_files_list(file_path,filelist)
    return filelist

def rec_files_list(file_path,filelist):
    for path in os.listdir(file_path):
        full_path = os.path.join(file_path, path)
        if os.path.isdir(full_path):
            dirname = os.path.basename(full_path)
            if not dirname.startswith("."):
                rec_files_list(full_path,filelist)
        else :
            name, extension = os.path.splitext(full_path)
            if (extension==""):
                file_name = Path(name).stem
                if not file_name.startswith("."):
                    filelist.append(full_path)
    return filelist

import logging
from concurrent.futures import ThreadPoolExecutor

import grpc
import numpy as np

from outliers_pb2 import OutliersResponse
from outliers_pb2_grpc import OutliersServicer, add_OutliersServicer_to_server
import os

import os
import json
#from api.utils.enhance_datamapjson import json_zip
import logging
import os
import glob
import warnings
from pathlib import Path
from datamap_format import mlmd_to_datamap
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
    


class OutliersServer(OutliersServicer):
    def Detect(self, request, context):
        #logging.info('dataset: %s', request.metrics)
        #logging.info('detect request size: %d', len(request.metrics))
        # Convert metrics to numpy array of values only
        #print(os.getcwd())
        mlmd_file_loc = request.metrics[0].name
        #mlmd_file_loc = os.getcwd()+"/mlmd/"
        JSON_CMF_REPO = os.getcwd()+"/json_repo/"
        mlmd_file_list = getmlmdlist(mlmd_file_loc)
        for mlmdfile in mlmd_file_list:
            if os.path.exists(mlmdfile):
                response_data, status = mlmd_to_datamap(mlmdfile,JSON_CMF_REPO)
                print("status",status)
                if not response_data:
                    print("NO RESPONSE DATA")
                    message = "Unknown Error - Not able to parse"+" : "+mlmdfile
                    print("message")
                    continue
                #print(type(response_data))
                print(response_data)    
                #OutliersResponse(indices = int(response_data))
                arr = bytes(response_data, 'utf-8')
                #print(arr)

        return OutliersResponse(indices = arr)


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
    )
    server = grpc.server(ThreadPoolExecutor())
    add_OutliersServicer_to_server(OutliersServer(), server)
    port = 9999
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    logging.info('server ready on port %r', port)
    server.wait_for_termination()

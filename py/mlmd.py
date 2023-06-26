#from api.plugin_utils import read_plugin
#from api.messaging.kafka_utils import send_message
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


def load_plugin(plugin_type, plugin_name):
    plugin_imp = os.path.join(settings.PLUGIN_DIRECTORY, plugin_type, plugin_name, "main.py")
    spec = importlib.util.spec_from_file_location(plugin_name, plugin_imp)
    plugin = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(plugin)
    return plugin.get_plugin()

def read_plugin(plugin_type, plugin_id):
    PLUGIN_DIRECTORY = os.environ.get("PLUGIN_DIRECTORY","plugins")
    search_dir = PLUGIN_DIRECTORY
    plugin_location = os.path.join(search_dir, plugin_type, plugin_id)
    if os.path.exists(os.path.join(plugin_location, "manifest.json")):
        plugin = load_plugin(plugin_type, plugin_id)
        return plugin
    else:
        return None



def abc():
    print(os.getcwd())
    mlmd_file_loc = os.getcwd()+"/mlmd/"
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
            #print(response_data)    
            response_data = json.loads(response_data)    
abc()
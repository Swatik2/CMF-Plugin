from django.http import JsonResponse
from rest_framework.parsers import JSONParser
from rest_framework.views import APIView
from api.plugin_utils.utils import read_plugin
from api.messaging.kafka_utils import send_message
from django.conf import settings
import os
import json
from api.utils.enhance_datamapjson import json_zip
from rest_framework import permissions

class InspectDataset(APIView):
    pass

class GetPluginList(APIView):
    pass

class GetPluginInfo(APIView):
    pass


class Getmlmd(APIView):
    permission_classes = [permissions.IsAuthenticated]
    def get(self, request, filename):
        try:
            # print(MLMDFILEPATH)
            plugin = read_plugin(settings.INSPECT_PLUGIN, settings.INSPECT_PLUGIN_FOLDER)
            # print(plugin)
            mlmdfile=os.path.join(settings.MLMDFILEPATH, filename)
            #checking if the file exist or not, if file not exist it will give 404 with error file not found
            if os.path.exists(mlmdfile):
                response_data, status = plugin.convert_mlmd_to_datamap(mlmdfile,settings.JSON_CMF_REPO)
                if not response_data:
                    if status == 400:
                        message = "MLMD database version 8 is greater than library version 7. Please upgrade the library"
                    else:
                        message = "Unknown Error"
                    return JsonResponse({"status": "failed", "message":message}, status=status)  
                # response_data = recursive_items(json.loads(response_data))
                response_data = json.loads(response_data)
                with open ("producer_data.json", "w") as out_file:
                   json.dump(response_data,out_file)
                print("kafka start------")
                send_message(topic=settings.KAFKA_TOPIC_INSPECT_META, message=json.dumps(response_data), key=settings.KAFKA_KEY)
                print("data send to kafka----")
            else:
                return JsonResponse({"status": "failed", "message": "file not found"}, status=404)
            # print("response----",response_data)
            return JsonResponse({"status": "success"}, status=200)
        except Exception as e:
            print(e)
            #for any other error
            return JsonResponse({"status": "failed", "message": "unknown error"}, status=500)
            
class Getmlmdlist(APIView):
    permission_classes = [permissions.IsAuthenticated]
    def post(self, request):
        try:
            mlmd_file_loc = request.data['edfLocation']
            project_id = request.data['projectId']
            if os.path.exists(mlmd_file_loc):
                plugin = read_plugin(settings.INSPECT_PLUGIN, settings.INSPECT_PLUGIN_FOLDER)
                print(mlmd_file_loc)
                print("WRITING FILE NAMES.........")
                mlmd_file_list=plugin.get_mlmd_file_list(mlmd_file_loc)
                print(mlmd_file_list)
                print("**********DONE************")         
                #checking if the file exist or not, if file not exist it will give 404 with error file not found
                for mlmdfile in mlmd_file_list:
                    # mlmdfile=mlmd_file_loc+"/"+file
                    if os.path.exists(mlmdfile):
                        response_data, status = plugin.convert_mlmd_to_datamap(mlmdfile,settings.JSON_CMF_REPO)
                        print("status",status)
                        if not response_data:
                            print("NO RESPONSE DATA")
                            message = "Unknown Error - Not able to parse"+" : " +mlmdfile
                            print(message)
                            continue
                        response_data = json.loads(response_data)
                        response_data["projectId"] = project_id
                        with open ("producer_data.json", "w") as out_file:
                            json.dump(response_data,out_file)
                        print("kafka start------")
                        send_message(topic=settings.KAFKA_TOPIC_INSPECT_META, message=json.dumps(response_data), key=settings.KAFKA_KEY)
                        print("data send to kafka----")
                    else:
                        print("File Not Found ..........:",mlmdfile)
                        continue
                return JsonResponse({"status": "success"}, status=200)
            else:
                return JsonResponse({"status": "failed", "message": "Path Does Not Exist"}, status=404)
        except Exception as e:
            print(e)
            #for any other error
            return JsonResponse({"status": "failed", "message": "unknown error"}, status=500)


class Getcsv(APIView):
    permission_classes = [permissions.IsAuthenticated]

    def get(self, request, filename):
        print(settings.INSPECT_PLUGIN, settings.INSPECT_CSV_PLUGIN_FOLDER)
        # headers = get_api_header(request)
        try:
            plugin = read_plugin(settings.INSPECT_PLUGIN, settings.INSPECT_CSV_PLUGIN_FOLDER)
            csvfile=os.path.join(settings.CSVFILEPATH, filename)
            #checking if the file exist or not, if file not exist it will give 404 with error file not found
            if os.path.exists(csvfile):
                response_data = plugin.csvMetadataExtraction(csvfile,settings.JSON_CSV_REPO)
                if not response_data:
                    return JsonResponse({"status": "failed", "message": "Unknown Error"}, status=500)
                # response_data = json.loads(response_data)
                print("JSON LOADING DONE")
                with open ("producer_csv_data.json", "w") as out_file:
                    print("writing to a file---")
                    json.dump(response_data,out_file)
                    # print(response_data)
                    print("DONE DONE DONE")
                print("kafka start------")
                send_message(topic=settings.KAFKA_TOPIC_CSV_INSPECT_META, message=json.dumps(response_data), key=settings.KAFKA_KEY)
                print("data send to kafka----")
            else:
                return JsonResponse({"status": "failed", "message": "file not found"}, status=404)
            # print("response----",response_data)
            return JsonResponse({"status": "success"}, status=200)
        except Exception as e:
            print(e)
            #for any other error
            return JsonResponse({"status": "failed", "message": "unknown error"}, status=500)

class Getcsvlist(APIView):
    permission_classes = [permissions.IsAuthenticated]

    def post(self, request):
        print(settings.INSPECT_PLUGIN, settings.INSPECT_CSV_PLUGIN_FOLDER)
        # headers = get_api_header(request)
        try:
            csv_file_loc = request.data['edfLocation']
            project_id = request.data['projectId']
            if os.path.exists(csv_file_loc):
                plugin = read_plugin(settings.INSPECT_PLUGIN, settings.INSPECT_CSV_PLUGIN_FOLDER)
                print(plugin)
                print("WRITING FILE NAMES.........")
                csv_file_list=plugin.get_csv_file_list(csv_file_loc)
                print("**********DONE************")
                for csvfile in csv_file_list:
                    # print("*******************************",type(csvfile))
                    # print("*******************************",type(str(csvfile)))
                    csvfile=str(csvfile)
                    # print("*******************************",type(csvfile))
                    if os.path.exists(csvfile):
                        response_data = plugin.csvMetadataExtraction(csvfile,settings.JSON_CSV_REPO)
                        if not response_data:
                            message = "Unknown Error - Not able to parse"+" : " +csvfile
                            print(message)
                            continue
                            # return JsonResponse({"status": "failed", "message": "Unknown Error"}, status=500)
                        response_data["resources"][0]["projectId"] = project_id
                        print("JSON LOADING")
                        # response_data = json.loads(response_data)
                        print("JSON LOADING DONE")
                        with open ("producer_csv_data.json", "w") as out_file:
                            print("writing to a file---")
                            json.dump(response_data,out_file)
                            # print(response_data)
                            print("DONE DONE DONE")
                        print("kafka start------")
                        send_message(topic=settings.KAFKA_TOPIC_CSV_INSPECT_META, message=json.dumps(response_data), key=settings.KAFKA_KEY)
                        print("data send to kafka----")
                    else:
                        return JsonResponse({"status": "failed", "message": "file not found"}, status=404)
                    # print("response----",response_data)
                return JsonResponse({"status": "success"}, status=200)
            else:
                return JsonResponse({"status": "failed", "message": "Path does not exist"}, status=404)

        except Exception as e:
            print(e)
            #for any other error
            return JsonResponse({"status": "failed", "message": "unknown error"}, status=500)
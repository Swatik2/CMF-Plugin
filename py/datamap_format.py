import json
import logging
import os

from cmflib import cmfquery
import warnings
warnings.filterwarnings("ignore")

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.DEBUG)

json_schema = '''
{
  "datapackage": {
    "licenses": [
      {
        "name": "",
        "title": "",
        "path": ""
      }
    ],
    "name": "",
    "sources": [
      {
        "title": "",
        "connectionUrl": "",
        "path": "",
        "email": ""
      }
    ],
    "resources": [
      {
        "name": "",
        "execution_id":"",
        "path": "",
        "version": "",
        "encoding": "",
        "mediatype": "",
        "homepage": "",
        "description": "",
        "title": "",
        "resourcetype": "",
        "bytes": "null",
        "data": {
          "propertyOrder": null,
          "title": "",
          "description": ""
        },
        "sources": [
          {
            "title": "",
            "path": "",
            "email": ""
          }
        ],
        "licenses": [
          {
            "path": "",
            "title": ""
          }
        ],
        "format": "",
        "schema": {},
        "hash": "",
        "profile": {
          "created": "",
          "numcolumns": null,
          "rowcount": null,
          "statistical": [
          ],
          "quality": [
              {}
          ],
          "tests": [
            {
              "testcase": {
                "type": "",
                "config": {
                  "commodo8e_": false,
                  "dolore3": false,
                  "reprehenderit_1": "sit ipsum cupidatat",
                  "officiac": 73666967
                }
              }
            }
          ]
        }
      }
    ],
    "title": "",
    "lineage": [{
      "run": {
        "runId": "",
        "facets": {
          "do9": {
            "_producer": "",
            "_schemaURL": ""
          }
        }
      },
      "job": {
        "namespace": "",
        "name": "",
        "facets": {
          "do__": {
            "_producer": "",
            "_schemaURL": ""
          }
        }
      },
      "eventTime": "",
      "producer": "",
      "schemaURL": "",
      "inputs": [
        {}
      ],
      "outputs": [
        {}
      ],
      "eventType": ""
    }],
    "id": "",
    "created": "",
    "contributors": [
      {
        "title": "",
        "role": "",
        "path": "",
        "email": ""
      }
    ],
    "keywords": [
      "et",
      "dolor",
      "velit elit quis officia ex",
      "ut velit consectetur"
    ],
    "homepage": "",
    "profile": "",
    "description": "",
    "image": ""
  }
}
'''


class CmfLib(object):
    def __init__(self, source="./mlmd", data_dict=None):
        # Extend Args Later if needed.
        self.query = cmfquery.CmfQuery(source)
        self.datapackage = ConstructJsonHelper(json.loads(json_schema), data_json=data_dict)

    def data_package_format(self):
        # Pipeline Names
        pipeline_names = self.query.get_pipeline_names()
        logging.info('Get Pipeline Names: %s', pipeline_names)

        # Pipeline ID, Stages
        for pipeline_name in pipeline_names:
            self.datapackage.add_name(pipeline_name)
            logging.info('Get Stages, ID for Pipeline: %s', pipeline_name)
            pipeline_stages = self.query.get_pipeline_stages(pipeline_name)
            pipeline_id = self.query.get_pipeline_id(pipeline_name)
            logging.debug("Pipeline Name, Stage, Id: (%s, %s, %s)" % (pipeline_name, pipeline_id, pipeline_stages))
            # self.datapackage.add_lineage(**self.datapackage.add_job(name=pipeline_name))
            # Count of Stages
            count = len(pipeline_stages)
            logging.info("Finding Executions in Stage")
            # Executions in Stage:
            for stage in pipeline_stages:
                logging.debug("Pipeline Stage: %s" % stage)
                executions = self.query.get_all_executions_in_stage(stage)
                # print(executions)
                total_exec_cols = executions.columns.to_list()

                job_cols = [job_ele for job_ele in total_exec_cols if
                            job_ele == "Git_End_Commit" or job_ele == "Git_Start_Commit" or job_ele == "process"]
                run_cols = [run_ele for run_ele in total_exec_cols if
                            run_ele != "Git_End_Commit" and run_ele != "Git_Repo" and run_ele != "Git_Start_Commit" and run_ele != "process"]

                logging.info("Executions Columns: %s" % executions.columns)
                logging.debug("Executions in Stage: %s" % executions)

                logging.info("Finding Artifacts in an Execution")
                for range_val in range(len(executions["id"])):
                    execution_id = executions["id"][range_val]
                    # index = executions.RangeIndex(execution_id)
                    logging.debug("Execution-ID: %s" % execution_id)

                    obj_run_facet = {"cmfRunFacets": dict()}
                    for i in run_cols:
                        key = ConstructJsonHelper.convert_to_camel_case(i)
                        if str(executions.dtypes[i]) == "int64":
                            obj_run_facet["cmfRunFacets"][key] = int(executions[i][range_val])
                        else:

                            obj_run_facet["cmfRunFacets"][key] = executions[i][range_val]

                    # job columns
                    obj_job_facet = dict()
                    obj_job_facet["sourceCodeLocation"] = dict()
                    obj_job_facet["cmfJobFacets"] = dict()
                    obj_job_facet["sourceCodeLocation"]["type"] = "git"
                    obj_job_facet["sourceCodeLocation"]["url"] = executions["Git_Repo"][range_val]
                    for i in job_cols:
                        key = ConstructJsonHelper.convert_to_camel_case(i)
                        if str(executions.dtypes[i]) == "int64":
                            obj_job_facet["cmfJobFacets"][key] = int(executions[i][range_val])
                        else:
                            obj_job_facet["cmfJobFacets"][key] = executions[i][range_val]
                    artifacts = self.query.get_all_artifacts_for_execution(execution_id)
                    print("**********************************************")
                    logging.debug("Artifacts: %s" % artifacts)
                    logging.debug("Artifacts Columns: %s" % artifacts.columns)
                    

                    for index, artifact in artifacts.iterrows():
                        # print(artifact)
                        name = artifact['name']
                        # if artifact['Commit'] == artifact["Commit"]:
                        obj_job_facet["sourceCodeLocation"]["version"] = artifact["Commit"]
                        if str(obj_job_facet["sourceCodeLocation"]["version"]) == 'nan':
                            obj_job_facet["sourceCodeLocation"]["version"] = ""
                        self.datapackage.add_resources(name=name, execution_id=int(execution_id),
                                                       data=artifact.to_json(), encoding="utf-8")
                                                    

                    # InputDataset
                    input_data_list = []
                    output_data_list = []

                    output_df = artifacts[artifacts['event'] == 'OUTPUT']
                    for index, artifact in output_df.iterrows():
                        obj = dict()
                        # name_data = artifact['name']
                        # name_temp = name_data.split(".csv")[0] + ".csv"
                        obj["facets"] = dict()

                        if artifact["type"] == "Dataset":
                            obj["facets"]["schema"] = dict()
                            obj["facets"]["cmfDataSetFacets"] = dict()
                            y = obj["facets"]["schema"]["fields"] = []
                            obj["facets"]["schema"]["stringDomain"] = []
                            # print(f"=========Output name_temp {name_temp} ====")
                            for i in artifacts.columns.to_list():
                                key = ConstructJsonHelper.convert_to_camel_case(i)
                                if i not in obj["facets"]["cmfDataSetFacets"] and i != 'name' and str(artifact[i]) != 'nan':
                                    if str(artifacts.dtypes[i]) == "int64":
                                        obj["facets"]["cmfDataSetFacets"][key] = int(artifact[i])
                                    else:
                                        obj["facets"]["cmfDataSetFacets"][key] = artifact[i]

                        elif artifact["type"] == "Model":
                                obj["facets"]["schema"] = dict()
                                obj["facets"]["schema"]["fields"] = [{
                                    "name": "modelProperties",
                                    "type": "TBD"
                                }]
                                obj["facets"]["cmfModelFacets"] = dict()
                                for i in artifacts.columns.to_list():
                                    key = ConstructJsonHelper.convert_to_camel_case(i)
                                    if i not in obj["facets"]["cmfModelFacets"] and str(artifact[i]) != 'nan':
                                        if artifact[i] == artifact[i]:
                                            if str(artifacts.dtypes[i]) == "int64":
                                                obj["facets"]["cmfModelFacets"][key] = int(artifact[i])
                                            else:

                                                obj["facets"]["cmfModelFacets"][key] = artifact[i]
                                        else:
                                            obj["facets"]["cmfModelFacets"][key] = "None"

                        else:
                            obj["facets"]["schema"] = dict()
                            obj["facets"]["schema"]["fields"] = [{
                                "name": "metricProperties",
                                "type": "TBD"
                            }]
                            obj["facets"]["cmfMetricsFacets"] = dict()
                            for i in artifacts.columns.to_list():
                                key = ConstructJsonHelper.convert_to_camel_case(i)
                                if i not in obj["facets"]["cmfMetricsFacets"] and str(artifact[i]) != 'nan':
                                    if artifact[i] == artifact[i]:
                                        if str(artifacts.dtypes[i]) == "int64":
                                            obj["facets"]["cmfMetricsFacets"][key] = int(artifact[i])
                                        else:

                                            obj["facets"]["cmfMetricsFacets"][key] = artifact[i]

                                    else:
                                        obj["facets"]["cmfMetricsFacets"][key] = "None"

                        name_spl = artifact["name"]
                        # print(artifact.get('git_repo', "git_repo not found"))
                        # name_spl = name_spl.replace("/", "//")
                        obj["name"] = name_spl
                        obj["namespace"] = pipeline_name

                        output_data_list.append(obj)

                    input_df = artifacts[artifacts['event'] == 'INPUT']
                    for index, artifact in input_df.iterrows():
                        obj = dict()
                        # name_data = artifact['name']
                        # name_temp = name_data.split(".csv")[0] + ".csv"
                        obj["facets"] = dict()

                        if artifact["type"] == "Dataset":
                            obj["facets"]["schema"] = dict()
                            obj["facets"]["cmfDataSetFacets"] = dict()
                            obj["facets"]["schema"]["fields"] = []
                            obj["facets"]["schema"]["stringDomain"] = []
                            for i in artifacts.columns.to_list():
                                key = ConstructJsonHelper.convert_to_camel_case(i)
                                if i not in obj["facets"]["cmfDataSetFacets"] and i != "name" and str(artifact[i]) != 'nan':
                                    if str(artifacts.dtypes[i]) == "int64":
                                        obj["facets"]["cmfDataSetFacets"][key] = int(artifact[i])
                                    else:
                                        obj["facets"]["cmfDataSetFacets"][key] = artifact[i]

                        elif artifact["type"] == "Model":
                                obj["facets"]["schema"] = dict()
                                obj["facets"]["schema"]["fields"] = [{
                                    "name": "modelProperties",
                                    "type": "TBD"
                                }]
                                obj["facets"]["cmfModelFacets"] = dict()
                                for i in artifacts.columns.to_list():
                                    key = ConstructJsonHelper.convert_to_camel_case(i)
                                    if i not in obj["facets"]["cmfModelFacets"] and str(artifact[i]) != 'nan':
                                        if artifact[i] == artifact[i]:
                                            if str(artifacts.dtypes[i]) == "int64":
                                                obj["facets"]["cmfModelFacets"][key] = int(artifact[i])
                                            else:
                                                obj["facets"]["cmfModelFacets"][key] = artifact[i]
                                        else:
                                            obj["facets"]["cmfModelFacets"][key] = "None"
                        else:
                            obj["facets"]["schema"] = dict()
                            obj["facets"]["schema"]["fields"] = [{
                                "name": "metricProperties",
                                "type": "TBD"
                            }]
                            obj["facets"]["cmfMetricsFacets"] = dict()
                            for i in artifacts.columns.to_list():
                                if i not in obj["facets"]["cmfMetricsFacets"] and str(artifact[i]) != 'nan':
                                    key = ConstructJsonHelper.convert_to_camel_case(i)
                                    if artifact[i] == artifact[i]:
                                        if str(artifacts.dtypes[i]) == "int64":
                                            obj["facets"]["cmfMetricsFacets"][key] = int(artifact[i])
                                        else:

                                            obj["facets"]["cmfMetricsFacets"][key] = artifact[i]
                                    else:
                                        obj["facets"]["cmfMetricsFacets"][key] = "None"

                        name_spl = artifact["name"]
                        # name_spl = name_spl.replace("/", "//")
                        obj["name"] = name_spl
                        obj["namespace"] = pipeline_name
                        input_data_list.append(obj)

                    self.datapackage.add_lineage(
                        **self.datapackage.add_item("run", runId=int(execution_id), facets=obj_run_facet),
                        inputs=input_data_list, outputs=output_data_list,
                        **self.datapackage.add_item("job", name=stage, namespace=pipeline_name, facets=obj_job_facet),
                        eventType="COMPLETE", eventTime="2005-03-12T18:30:00.0Z", producer="producer")

                    if "statistics" in artifacts.columns:
                        # json_temp_stat = json.loads(artifacts.get('statistics')[0])
                        json_temp_stat = json.loads(artifacts.get('statistics').to_json())
                        self.datapackage.add_profile(statistical=[json_temp_stat])
                    if "schema" in artifacts.columns:
                        json_temp_schema = json.loads(artifacts['schema'].to_json())
                        self.datapackage.add_resources(schema=json_temp_schema)
                    if "quality" in artifacts.columns:
                        pass

            self.datapackage.add_summary(count)


class ConstructJsonHelper(object):
    def __init__(self, datapackage, data_json=None):
        self.datapackage_schema = datapackage["datapackage"]
        self.datapackage = data_json if data_json else {}

    def add_name(self, name):
        self.datapackage["name"] = name

    def add_title(self, title):
        self.datapackage["title"] = title

    def add_id(self, id):
        self.datapackage["id"] = id

    def add_homepage(self, homepage):
        self.datapackage["homepage"] = homepage

    def add_image(self, image):
        self.datapackage["image"] = image

    def add_description(self, description):
        self.datapackage["description"] = description

    def add_summary(self, count, offset=0):
        self.datapackage["count"] = count
        self.datapackage["total"] = count
        self.datapackage["offset"] = offset

    def add_job(self, **job_params):
        return {"job": job_params}

    def add_item(self, item_name, **job_params):
        return {item_name: job_params}

    def add_keywords(self, keywords):
        self.datapackage["keywords"] = keywords

    def add_resources(self, **kwargs):
        keys = list()
        for key, value in self.datapackage_schema["resources"][0].items():
            keys.append(key)
        resources_dict = self.add_values(keys, **kwargs)
        self.datapackage.setdefault("resources", []).append(resources_dict)

    def add_profile(self, **kwargs):
        keys = list()
        for key, value in self.datapackage_schema["resources"][0]["profile"].items():
            keys.append(key)
        profile_dict = self.add_values(keys, **kwargs)
        self.datapackage.setdefault("resources", [])[0].setdefault("profile", {}).update(profile_dict)

    def add_lineage(self, **kwargs):
        keys = list()
        for key, value in self.datapackage_schema["lineage"][0].items():
            keys.append(key)
        lineage_dict = self.add_values(keys, **kwargs)
        self.datapackage.setdefault("lineage", []).append(lineage_dict)

    def add_contributors(self, **kwargs):
        keys = list()
        for key, value in self.datapackage_schema["contributors"][0].items():
            keys.append(key)
        contributors_dict = self.add_values(keys, **kwargs)
        self.datapackage.setdefault("contributors", []).append(contributors_dict)

    def add_licences(self, **kwargs):
        # read args and add to licenses, just a structure below for single dict.
        # read this once, etc .. keep schema separate from data etc. enhancements.
        keys = list()
        for key, value in self.datapackage_schema["licenses"][0].items():
            keys.append(key)
        license_dict = self.add_values(keys, **kwargs)
        self.datapackage.setdefault("licenses", []).append(license_dict)

    def add_values(self, keys, **kwargs):
        values = dict()
        for key, value in kwargs.items():
            if key in keys:
                values[key] = value
        return values

    def add_sources(self, **kwargs):
        # read args and add to license, just a structure below for single dict.
        keys = list()
        for key, value in self.datapackage_schema["sources"][0].items():
            keys.append(key)
        sources_dict = self.add_values(keys, **kwargs)
        self.datapackage.setdefault("sources", []).append(sources_dict)

    def printjson(self):
        # May not be needed since creating a new dict now.
        # print(json.dumps(self.datapackage['lineage'], indent = 1))
        return self.del_none(self.datapackage)

    def del_none(self, d):
        """
        Delete keys with the value ``None``, "" in a dictionary, recursively.
        This alters the input so you may wish to ``copy`` the dict first.
        """
        for key, value in list(d.items()):
            if value is None:
                del d[key]
            elif isinstance(value, dict):
                self.del_none(value)
        return d

    def validate_json(self):
        # TODO: validate the json against the schema provided.
        # import jsonschema
        # jsonschema.validate()
        pass


    @staticmethod
    def convert_to_camel_case(string, char='_'):
        mappings = {"createTimeSinceEpoch": "createdAt", "lastUpdateTimeSinceEpoch": "updatedAt"}
        words = string.split(char)
        camel_case = "".join([words[0].lower()]+[i.capitalize() for i in words[1:]]) if len(words) > 1 else string.lower()
        return mappings.get(camel_case, camel_case)


def filter_old_pipelines(data):
    """
    Method to filter the old executions of each stage in pipeline and keep stages of the latest execution
    only (Identified with maximum runId in each stage).
    """
    # Initialization
    output = {}
    for i in data['lineage']:
        # Making (Job name, inputs artifacts, outputs artifacts) as key
        key = (i["job"]["name"], "inputs", *[j["name"] for j in i["inputs"]],
            "outputs", *[k["name"] for k in i["outputs"]])
            # If key is already present and runId is smaller than existing one, skip it.
        if key in output and output[key]['run']['runId'] > i["run"]["runId"]:
            continue
        # Adding lineage to the dictionary
        output[key] = i

    # Take out all the lineages filtered
    data['lineage'] = list(output.values())
    return data

def mlmd_to_datamap(source,json_repository):
    try:
        # Serialize json
        cmflib = CmfLib(source=source)
        cmflib.data_package_format()
        datapackage_json = json.dumps(
            filter_old_pipelines(cmflib.datapackage.printjson()), sort_keys=True, indent=4
        )

        # Store DataMap Format in JSON
        repo, file_name = os.path.split(source)
        # json_repo = os.path.join(os.path.split(repo)[0], "JSONFiles")
        json_repo=json_repository
        
        if not os.path.exists(json_repo):
            os.makedirs(json_repo)
        json_path = os.path.join(json_repo, f"{file_name}.json")
        with open(json_path, "w") as fp:
            fp.write(datapackage_json)

        return datapackage_json, 200

    # except RuntimeError as e:
    #     # Logging error and returning empty object
    #     logging.error(f"Unknown error in processing {source} file - {e}")
    #     return None, 400
    except Exception as e:
        # Logging error and returning empty object
        logging.error(f"Unknown error in processing {source} file - {e}")
        return None, 500

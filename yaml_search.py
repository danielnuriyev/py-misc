import os
import yaml

def load(directory):
 
 parsed_data = []

 for root, _, files in os.walk(directory):
   for file in files:
     if file.endswith(".yaml"):
       file_path = os.path.join(root, file)

       try:
         with open(file_path, "r") as stream:
           source = stream.read()
           yaml_obj = yaml.safe_load(source)
           parsed_data.append({
             "file": file,
             "path": file_path,
             "object": yaml_obj,
             "source": source
           })
       except yaml.YAMLError as exc:
         print(f"Error parsing YAML file '{file_path}': {exc}")

 parsed_data = sorted(parsed_data, key=lambda x: x["path"]) 
 return parsed_data

def count_raw(objects):

  found = []

  objects.sort(key=lambda x: x["path"])

  for element in objects:

    object = element["object"]
    
    if "skip" in object and object["skip"]:
      continue

    # if "ingest_method" in object and object["ingest_method"] in {"reloaded", "appended", "streamed"}:
    #  continue

    if "dependencies" in object:
      continue

    if "steps" in object:
      for step in object["steps"]:
        if step.get("type",None) == "source":
          if "resource" in step and step["resource"] != "athena_query_extract":
            found.append(element["path"])
            break
        if step.get("type",None) == "link" and step.get("op",None) == "create_table":
          if step.get("config",{}).get("storage_folder",None) != None:
            found.append(element["path"])
            break
        
  return found

def count_sinks(objects):

  found = []

  objects.sort(key=lambda x: x["path"])

  for element in objects:

    object = element["object"]
    
    if "skip" in object and object["skip"]:
      continue

    if "steps" in object:
      for step in object["steps"]:
        if step.get("type",None) == "sink":
          if "resource" in step and step["resource"] not in ("athena", "store_google_sheet", "email_users_csv", "to_excel"):
            found.append(step["resource"])
            break
        
  return found

def search(objects):

  for element in objects:

    object = element["object"]
    
    if object.get("skip",False):
      continue

    if object.get("ingest_method", None) == "appended":
      for step in object.get("steps",[]):
        if step.get("type",None) == "sink" and step.get("resource",None) == "athena":
          if step.get("config",{}).get("merge_method", None) == None:
            print(element["path"])
            break
  

directory = "/Users/daniel.nuriyev/projects/data-platform/dagster"
objects = load(directory)
# results = count_raw(objects)
# results = count_sinks(objects)
results = search(objects)
print(results)

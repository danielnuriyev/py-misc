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
           yaml_obj = yaml.safe_load(stream)
           yaml_obj["file"] = file
           # print(yaml_obj)
           parsed_data.append({
              "path": file_path,
              "object": yaml_obj,
           })
       except yaml.YAMLError as exc:
         print(f"Error parsing YAML file '{file_path}': {exc}")

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

def search(objects):

  max_steps = 0
  found = None

  for element in objects:

    object = element["object"]
    
    if "skip" in object and object["skip"]:
      continue

    if "steps" in object:
      if len(object["steps"]) > max_steps:
        max_steps = len(object["steps"])
        found = element["path"]
      
  return [(found,max_steps)]

directory = "/Users/daniel.nuriyev/projects/data-platform/dagster"
objects = load(directory)
results = count_raw(objects)

for result in results:
  print(result)

print(len(results))

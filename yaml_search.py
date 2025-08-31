import os
import yaml

import bedrock

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

    if "dependencies" in object:
      continue

    """
    if object.get("execution", None) != None:
      if object["execution"].get("capacity", None) == "on_demand":
          continue
      elif object["execution"].get("type", None) == "fargate":
          continue
    """
    
    if "steps" in object:
      for step in object["steps"]:
        if step.get("type",None) == "source":
          if "resource" in step and step["resource"] != "athena_query_extract":
            # found.append(element["path"])
            found.append(element["file"][:-5])
            break
        if step.get("type",None) == "link" and step.get("op",None) == "create_table":
          if step.get("config",{}).get("storage_folder",None) != None:
            # found.append(element["path"])
            found.append(element["file"][:-5])
            break
        
  return found

def count_sources(objects):

  found = set()

  objects.sort(key=lambda x: x["path"])

  for element in objects:

    object = element["object"]
    
    if "skip" in object and object["skip"]:
      continue

    if "dependencies" in object:
      continue

    if "steps" in object:
      for step in object["steps"]:
        if step.get("type",None) == "source":
          if "resource" in step and step["resource"] != "athena_query_extract":
            found.add(step["resource"])
            break
        if step.get("type",None) == "link" and step.get("op",None) == "create_table":
          if step.get("config",{}).get("storage_folder",None) != None:
            found.add("s3")
            break
        
  return found

def count_dependent(objects):

  found = []

  objects.sort(key=lambda x: x["path"])

  for element in objects:

    object = element["object"]
    
    if "skip" in object and object["skip"]:
      continue

    if "dependencies" in object:
      found.append(element["path"])
        
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

  found = set()

  for element in objects:

    object = element["object"]
    
    if object.get("skip", False):
      continue

    """
    if object.get("execution", None) != None:
      if object["execution"].get("capacity", None) == "on_demand":
          continue
      elif object["execution"].get("type", None) == "fargate":
          continue
    """

    found.add(element["file"][:-5])

  return found

  return found
      
def bedrock_search(objects, question):

  chat = bedrock.Chat("local")

  for element in objects:

    source = element["source"]

    prompt = f"""
    Do this: {question} regarding this content:

    {source}
    """


directory = "/Users/daniel.nuriyev/projects/data-platform/dagster"
results = load(directory)

# results = count_sources(results)
results = count_raw(results)
# results = count_dependent(results)
# results = count_sinks(results)
# results = search(results)

print(list(results))
# print(len(results))

# bedrock_search(objects, question)

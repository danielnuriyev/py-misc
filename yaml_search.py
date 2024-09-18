import os
import yaml

def load(directory):
 """Walks a directory, parses YAML files, and collects parsed data in a list.

 Args:
   directory: The path to the directory to walk.

 Returns:
   A list of parsed YAML data.
 """

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
           parsed_data.append(yaml_obj)
       except yaml.YAMLError as exc:
         print(f"Error parsing YAML file '{file_path}': {exc}")

 return parsed_data

def search(objects):

  count = 0

  for object in objects:
    
    if "skip" in object and object["skip"]:
      continue

    if "dependencies" in object:
      continue  
      
    if "steps" in object:
      _continue = False
      for step in object["steps"]:
        if step["type"] == "source" and step["resource"] == "athena_query_extract":
          _continue = True
          break
      if _continue:
        continue

    count += 1

  return count

directory = "/Users/daniel.nuriyev/projects/data-platform/dagster"
objects = load(directory)
results = search(objects)
print(results)

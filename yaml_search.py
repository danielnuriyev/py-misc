import os
import yaml

def parse_yaml_files(directory):
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

def find_max_memory_file(data):

  max_memory_files = []
  max_memory = 0

  for item in data:
    if "resources" in item and "memory" in item["resources"]:
      memory_value = int(item["resources"]["memory"][0:-2])
      if memory_value > max_memory:
        max_memory_files = [item]
        max_memory = memory_value
      elif memory_value == max_memory:
        max_memory_files.append(item)

  return max_memory_files

# Example usage:
directory = "/Users/daniel.nuriyev/projects/data-platform/dagster"  # Replace with the actual directory path
parsed_yaml_data = parse_yaml_files(directory)
max_memory_files = find_max_memory_file(parsed_yaml_data)
for file in max_memory_files:
  print(f"{file["file"]} : {file["resources"]["memory"]}")



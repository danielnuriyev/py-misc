import os
import yaml

def merge_yaml_files(directory, output_file):
    merged_content = ""

    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.yaml') or file.endswith('.yml'):
                file_path = os.path.join(root, file)
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = yaml.safe_load(f)
                    merged_content += f"\n\n{file}\n\n" + yaml.dump(content)

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(merged_content)

directory_path = "/Users/daniel.nuriyev/projects/data-platform/dagster/"
output_file_path = "/Users/daniel.nuriyev/projects/merged.yaml"
merge_yaml_files(directory_path, output_file_path)
print(f"Merged YAML content saved to {output_file_path}")

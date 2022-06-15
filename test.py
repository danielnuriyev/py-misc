
import random
import time
import uuid


def generate_folders(count):
    """
    Generates random folder names
    :param count: number of folder names to generate
    :return: random folder names
    """
    tables = []
    for i in range(count):
        tables.append(str(uuid.uuid4()))
    return tables


def generate_files(folders, count):
    """
    Generates file paths belonging to the specified list of folders
    :param folders: list of folders
    :param count: total number of file paths in all the folders
    :return: file paths belonging to the specified list of folders
    """
    files = []
    for i in range(count):
        folder = folders[random.randint(0, len(folders) - 1)]
        file = f"{folder}/{uuid.uuid4()}"
        files.append(file)
    return files


def setup():
    """
    This code generates a list of file paths belonging to a set of folders
    """
    t = time.time()
    folders = generate_folders(10000)
    files = generate_files(folders, 1000000)
    random.shuffle(files)
    print(len(files))
    print(time.time() - t)

    """
    This code defines a list of valid fodlers
    """
    valid_folders = folders[0:1000]

    return valid_folders, files


def find_invalid_folders(files, valid_folders):
    """
    Returns a list of invalid folders
    :param files: list of file paths
    :param valid_folders: list of valid folders
    :return: list of invalid folders
    """

    # TODO: IMPLEMENT


"""
CONDITIONS: 
You are given a list of valid folders and a list of files.
Some of the files belong to the valid folders and some don't, i.e. they belong to the invalid folders.

TASK:
Find the invalid folders: implement find_invalid_folders function defined above
"""

valid_folders, files = setup()

t = time.time()
find_invalid_folders(files, valid_folders)
print(time.time() - t)

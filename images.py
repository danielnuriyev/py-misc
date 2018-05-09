
import os
from PIL import Image

p = '/Users/dnuriyev/ml/train'

for f in os.listdir(p):
    print(f)

'''
to gray
    https://pillow.readthedocs.io/en/3.1.x/reference/Image.html#PIL.Image.Image.convert
to vector
'''
"""
g = 2022
for h in reversed(range(1, 5784)):

    if g in [-3631, -1, 0, 1, 2022] or h == 1:
        print(f"{g} - {h}")

    g = g - 1
    if g == 0:
        g = -1
"""
import datetime
print(datetime.datetime.now().timestamp())
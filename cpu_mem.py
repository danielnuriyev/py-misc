import os
import psutil
process = psutil.Process(os.getpid())
print(process.memory_full_info())
print(process.memory_percent())
print(process.cpu_percent())

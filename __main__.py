import os

#this is to execute the data_processing_script.
path = os.getcwd()
print(path)
with open(f'{path}/dataprocessing_script.py') as f:
    exec(f.read())

import os

getting_data_processing_script_path = os.getcwd()
path = getting_data_processing_script_path

print(path)
with open(f'{path}/dataprocessing_script.py') as f:
    exec(f.read())

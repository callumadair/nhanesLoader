import nhanesLoader as nl
from nhanesVariables import tests

import os
print(os.path.expanduser('~'))

testList = ["THYROD", "CBC"]
current_directory = "/home/cal/py_projects/nhanesLoader"
csvFile = '/home/cal/py_projects/nhanesLoader/data/output.csv'
nl.nhanes_merger_numpy(current_directory + "Nhanes\\", testList, destination=csvFile, all=True)  # Scrape and creates CSV
df = nl.load_csv(csvFile, ageMin=18, ageMax=25)  # Load the created CSV file into a dataframe

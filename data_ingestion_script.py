""""This is to ingest data from the cricsheet"""
from selenium import webdriver
from selenium.webdriver import chrome
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

from selenium.webdriver.common.keys import Keys
 
# def data_ingestion(type_of_match):
options= webdriver.ChromeOptions()
options.add_experimental_option("detach", True)
# create webdriver object
driver = webdriver.Chrome(options=options, service=Service(ChromeDriverManager().install()))

# get google.co.in
driver.get("https://cricsheet.org/downloads/")


# search_bar = driver.find_element_by_name("q")

# element = driver.find_element(By.XPATH, '//*[@id="main"]/div/table[2]/tbody/tr[5]/td[1]')
# name
login_form = driver.find_element(By.ID ,"name")
print(login_form)


# data_ingestion("")
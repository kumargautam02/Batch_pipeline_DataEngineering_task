""""This is to ingest data from the cricsheet"""
from selenium import webdriver
from selenium.webdriver import chrome
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.keys import Keys
 

options= webdriver.ChromeOptions()
options.add_experimental_option("detach", True)
# create webdriver object
driver = webdriver.Chrome(options=options, service=Service(ChromeDriverManager().install()))

# get google.co.in
driver.get("https://cricsheet.org/downloads/")


search_bar = driver.find_element_by_name("q")
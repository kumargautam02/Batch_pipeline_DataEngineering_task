# """"This is to ingest data from the cricsheet"""
from selenium import webdriver
from selenium.webdriver import chrome
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options


def download_required_files(logger):

    ########################################################################################################################        
    # This Python-function is used to create download the male.json and femal.json file from the cricsheet.org site. 
    # This function is using selenium chrome-webdriver to download the zip file in the download folder "C:\Users\Admin\Downloads".
    # arguments :
    #     logger - This is the logger object, used to log info.
    # return : 
    #     Nothing
    #########################################################################################################################
    
    chromeOptions= webdriver.ChromeOptions()
    chromeOptions.add_experimental_option("detach", True)
    driver = webdriver.Chrome(options = chromeOptions,service=Service(ChromeDriverManager().install()))
    driver.get("https://cricsheet.org/downloads/")
    x_path_for_male_dataset = '//*[@id="main"]/div/table[2]/tbody/tr[5]/td[4]/ul/li[1]/a'
    download_mail_dataset = driver.find_element(By.XPATH, x_path_for_male_dataset)
    download_mail_dataset.click()
    driver.implicitly_wait(120)
    logger.info("Download of Male_Dataset Started")
    x_path_for_female_dataset = '//*[@id="main"]/div/table[2]/tbody/tr[5]/td[3]/ul/li[1]/a'
    download_female_dataset = driver.find_element(By.XPATH, x_path_for_female_dataset)
    download_female_dataset.click()
    driver.implicitly_wait(120)
    logger.info("Download of Female_Dataset Started")
    return 
from urllib.parse import urlparse
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
from webdriver_manager.chrome import ChromeDriverManager

from concurrent.futures import ProcessPoolExecutor, wait

import re,os
import pandas as pd
import numpy as np
import openpyxl

def getChromeDriver():
    options = webdriver.ChromeOptions()
    options.add_argument('ignore-certificate-errors')
    #options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument('--blink-settings=imagesEnabled=false')

    prefs = {
        "download_restrictions": 3,
    }
    options.add_experimental_option(
        "prefs", prefs
    )

    driver=webdriver.Chrome(service=Service(ChromeDriverManager().install()),chrome_options=options) # define a chrome driver
    return driver

def getKeyWords(keywordFile):
    with open(keywordFile,'r', encoding="utf8") as kf:
        text = kf.read().splitlines()
        return text

def getUrls(UrlFile):
    df = pd.read_csv(UrlFile,encoding="utf8")
    url_list = df['Website'].dropna().tolist()
    return url_list
   
def getSoup(url):
    driver = getChromeDriver()
    soup =None
    get_url=''
    try:
        #driver.maximize_window()
        driver.get(url)
        get_url = driver.current_url
        WebDriverWait(driver, 1).until(EC.url_to_be(url))
        WebDriverWait(driver, 1).until(EC.element_to_be_clickable((By.XPATH,'//*[@id="cookie_action_close_header"]'))).click()
    except:
        pass

    if get_url == url:
        page_source = driver.page_source
        soup = BeautifulSoup(page_source,features='html.parser')

    driver.quit()
    return soup 

def findKeywords(url,keywords):
    soup = getSoup(url)
    file = open('results.txt', 'a+',encoding='utf8')
    try:
        if soup != None :
            dataRow=[]
            for keyword in keywords:
                matches = soup.body.find_all(string=re.compile(keyword, re.IGNORECASE))
                len_match = len(matches)
                if len_match > 0:
                    dataRow.append((urlparse(url).netloc,url,keyword,len_match))

            for t in dataRow:
                    file.write(','.join(str(s) for s in t) + '\n')

    except:
        pass       
    file.close()

def getHref(url):
    soup = getSoup(url)
    suburls=[]
    if soup != None :
        for link in soup.find_all('a', href=True):
                if link['href'] is not None and link['href'].startswith(("https:","http:")):
                    if urlparse(link['href']).netloc == urlparse(url).netloc:
                        if not link['href'] in suburls and not link['href'] in url and not link['href'].endswith((".png",".pdf",".jpg",".jpeg")):
                            suburls.append(link['href']+ '\n')  

        with open('urls.txt', 'a+') as f:
            f.writelines(url+ '\n')
            f.writelines(suburls)

    return suburls

def getAllHref(url):
    for link in getHref(url):
            getAllHref(link)

def webCrawler(keywords,urls):

    # list to store the processes
    processList = []

    # initialize the mutiprocess interface
    with ProcessPoolExecutor(os.cpu_count()-1) as executor:
        for url in urls:
            processList.append(executor.submit(getAllHref, url))

    # wait for all the threads to complete
    wait(processList)

    urlset = sorted(set(line.strip() for line in open('urls.txt')))
        
    file=open('results.txt', 'w+',encoding='utf8') # w+ means overwrite file and read, append 'a+' means add to file and read
    file.close()

    kprocessList = []
    with ProcessPoolExecutor(os.cpu_count()-1) as executor:
        for url in urlset:
            kprocessList.append(executor.submit(findKeywords,url,keywords))   
    wait(kprocessList)

    df = pd.read_csv('results.txt',encoding='utf8')
    df.columns = ["BaseUrl","Url","KeyWord","Matches"]
    dfpivot=df[['BaseUrl','KeyWord','Matches']].groupby(['BaseUrl', 'KeyWord'])['Matches'].sum().unstack('KeyWord',fill_value=0)
    dfpivot.to_excel('results.xlsx')
  

if __name__ == '__main__':
    keywordFile='keyword.txt'
    UrlFile='All investors.csv'
    keywords_lst = getKeyWords(keywordFile)
    urls_lst = getUrls(UrlFile)
    webCrawler(keywords_lst,urls_lst)
    
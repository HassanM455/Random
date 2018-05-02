from bs4 import BeautifulSoup as bs
from urllib.request import urlopen as uReq
import pandas as pd

# saving html address in variable
address = 'https://www.expowest.com/ew18/Public/Exhibitors.aspx?ID=1067805&sortMenu=115000'
# grabbing pointer referencing the site html code 
temp = uReq(address)
# reading into the variable 'site' the 'object' the pointer, in the variable temp, is pointing to
site = temp.read()
# closing the file in 'temp', so the content in the buffer may be dumped into the variable 'site'
temp.close()
# converting bytes object into bs4.BeautifulSoup object
soup = bs(site,'html.parser')
# cleaning up syntax with the prettify() method
code_text = soup.prettify()
# grabbing tag objects corresponding to rows of the table containing company names
nameRow_tags = soup.find_all('a' , class_='exhibitorName')

print('Number of company names is  : ', len(nameRow_tags))

# writing loop to put all names in the table in a list to be turned into a Data Frame object
names = []
for i in range(len(nameRow_tags)):
    names.append(nameRow_tags[i].text)
    

# create company name data frame
company_name = pd.DataFrame({'Company Names' : names})

# saving data frame in csv file in default directory
company_name.to_csv('Company_Names.csv')

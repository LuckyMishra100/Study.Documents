from bs4 import BeautifulSoup
import requests
import pandas as pd

name = []
rating = []
price = []

URL = requests.get("https://www.flipkart.com/search?q=laptop&otracker=search&otracker1=search&marketplace=FLIPKART&as-show=on&as=off")

soup = BeautifulSoup(URL.content,'html.parser')
# print(soup.prettify())
data = soup.find('div',class_ = '_1YokD2 _3Mn1Gg').find_all('div',class_ = '_1AtVbE col-12-12')

for i in data:
    divname = i.find('div',class_ = '_4rR01T')
    
    if divname is not None:
        name.append(divname.get_text())
    else:
        name.append("Null")



    divrate = i.find('div',class_ = '_3LWZlK')
    if divrate is not None:
        rating.append(divrate.get_text())
    else:
        rating.append(str('0'))
    

    divprice = i.find('div',class_ = '_30jeq3 _1_WHN1')
    if divprice is not None:
        price.append(divprice.get_text())
    else:
        price.append(0)

df = pd.DataFrame({"ProductName":name,"Price":price,"Rating":rating})
# df.to_csv('laptop.csv',header= True,index=False)
print(df)    

# print(price)

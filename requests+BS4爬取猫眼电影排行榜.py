
# coding: utf-8

# In[492]:


#description:a spider for grabbing some info of rank 100 movies on MAOYAN .
import requests
import pandas as pd
from bs4 import BeautifulSoup
from multiprocessing import Process
__author__ =  'Jonathan_Rowe'
__fstmdtime__ ='2019-03-19'
__lstmdtime__ ='2019-03-19'


# ### get web content

# In[493]:


def getUrl(url):
    try:
        response = requests.get(url)
        print('response=%s'%response) #just for testing 
        if response.status_code ==200:
            return response.text
        else:return None
    except:return None


# ### show all dataframe

# In[494]:


def showAll():
    # expand dataframe lenth
    pd.set_option('display.max_columns', 1000)
    pd.set_option('display.width', 1000)
    pd.set_option('display.max_colwidth', 1000)
    return


# ###  filter content by BS4

# In[495]:


def filter(res):
    try:
        soup = BeautifulSoup(res,'html.parser')  #only soup object has find_all function
        dd = soup.find_all(name='dd')  #get data block
        # print(dl)
        for item in dd:
            print('*************************************\n')

            # print('c=%s'%c)
            index = item.i.string
            title = item.find(class_='name').string
            actors = item.find(class_='star').string.strip()[3:]
            img = item.find(class_='board-img')['data-src']
            time = item.find(class_='releasetime').string[5:]
            score =item.find(class_='integer').string+item.find(class_='fraction').string
    #         print('%s\n'%index,'%s\n'%title,'%s\n'%actors,'%s\n'%img,'%s\n'%time,'%s\n'%score)  #just for testing
            data = [[title,score,time,actors,img]]
            exits=0
            if not exits:
                df = pd.DataFrame(data,index=[index],columns=['title','score','time','actors','img_link'])
                exits=1
            else:
                df.append(data)
            yield df
    except Exception as e:
        print(type(e),e)
        return None

    return None
        


# ###  save data as file

# In[496]:


def save(df):
    df.to_csv('MaoYanMovieRank.csv',mode='a')


# ### multiprocess main part  

# In[497]:


def main():
    for i in range(0, 100, 10):
            url = 'https://maoyan.com/board/4?offset=%s'%i
            res = getUrl(url)
            if type(res) != None:
                dfg = filter(res)
                for df in dfg:
                    print(df)   #for testing
                    save(df)
                


# ### main 

# In[498]:


if __name__ == '__main__':
    global exits,df
    showAll()
    p = Process(target=main)
    p.start()
    main()
    
    


import aiofiles
from bs4 import BeautifulSoup as bs
import csv
import httpx
from fake_useragent import UserAgent
import datetime
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import asyncio
import sys



class newsBenzinga:
       
    def __init__(self):
        # Время новости
        self.news_time = ''
        # Заголовок новости
        self.title = ''
        # ссылка на новость
        self.news_url = ''
        self.url = 'https://www.Benzinga.com/markets'

    # Считывание всех строк с файла .сsv для исключения повторений новостей
    async def except_url(self):
       try:
           all_rows = []
           async with aiofiles.open('./news.csv', 'r') as f: 
               async for line in f:
                    all_rows.extend(line.strip().split(","))            
       except FileNotFoundError:
            pass
       return all_rows

    # Функция будет записывать в файл полученные данные
    
    async def write_file(self, str_n):
        
        # Список одной строчки csv файла (время, заголовок, ссылка)
        csv_head = ['time', 'title', 'url']   
        async with aiofiles.open('./news.csv', 'a') as f:                    
                   
                  if csv_head[2] not in await self.except_url():
                       await f.write(f"{','.join(map(str, csv_head))}\n")   
                        
                  return await f.write(','.join(map(str, str_n)).replace('\n', '') + '\n')
     
    # Парсинг новостей                  
    async def parsing(self, HTML):
         
         str_n = []
         for i in range(len(HTML.select('div.post-card-text-wrapper a.post-card-article-link'))):    
                           news_time = (datetime.datetime.now().time())
                           title = (HTML.select('div.post-card-text-wrapper a.post-card-article-link')[i].text)
                           news_url = (self.url + HTML.select('div.post-card-text-wrapper a.post-card-article-link')[i]['href'])
                            
                           # Исключаем повторяющиеся новости и пустые строки      
                           if news_url not in await self.except_url():
                               str_n.append(news_time)
                               
                               str_n.append(str(title).replace(",", ''))
                               
                               str_n.append(news_url)
                               
                           if str_n != []:
                                 await self.write_file(str_n)
                                 print(str_n)
                           str_n = []



async def main():
    
    news_1 = newsBenzinga()
    # Установка соединения
    headers = {'User-agent': UserAgent().random}
    async with httpx.AsyncClient() as client:
        
        r = await client.get(url = news_1.url, headers = headers)
        print(r.status_code)
        html = bs(r.content, 'html.parser')
    
    
    # Задаю параметры для автообновления csv таблицы
    scheduler = AsyncIOScheduler()
    if len(sys.argv) < 2:
          print("Укажите время обновления в секундах.")
          sys.exit(1)
    
    time = int(sys.argv[1])
    print("Ваши новости обновляются каждую секунду.")
    
    scheduler.add_job(news_1.parsing, 'interval', seconds = time, args=[html])
    scheduler.start()
    await asyncio.Event().wait()

if __name__ == "__main__":
         asyncio.run(main())


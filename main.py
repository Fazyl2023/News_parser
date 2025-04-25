
from bs4 import BeautifulSoup as bs
import requests
import csv
from fake_useragent import UserAgent
import datetime
from apscheduler.schedulers.background import BackgroundScheduler


class newsBenzinga:
       
    def __init__(self, url: str):
        # Время новости
        self.news_time = ''
        # Заголовок новости
        self.title = ''
        # ссылка на новость
        self.news_url = ''
        self.url = url

    # Установка соединения
    def soed(self):
           headers = {
                    'User-agent': UserAgent().random
                    }
           response = requests.get(self.url, headers=headers)
           print(response.status_code)

           html = bs(response.content, 'html.parser')
           self.csv_head = ['time', 'title', 'url']
           return html
    
    # Считывание всех строк с файла .сsv для исключения повторений новостей
    def except_url(self):
       try:
           all_rows = []
           with open('./news.csv', 'r') as f: 
              read = csv.reader(f, delimiter=',')
              for row in read:
                  for i in row:
                      all_rows.append(i)
       except FileNotFoundError:
            pass
       return all_rows

    # Функция будет записывать в файл полученные данные
    def write_file(self):
        HTML = self.soed()
        # Список одной строчки csv файла (время, заголовок, ссылка)
        str_n = []
        # список нужен для того, чтобы новости не повторялись в csv
           
        with open('./news.csv', 'a') as f:                    
                   writer = csv.writer(f)
                   if self.csv_head not in self.except_url():
                       writer.writerow(self.csv_head)
               
                   for i in range(len(HTML.select('div.post-card-text-wrapper a.post-card-article-link'))):    
                           news_time = (datetime.datetime.now().time())
                           title = (HTML.select('div.post-card-text-wrapper a.post-card-article-link')[i].text)
                           news_url = (self.url + HTML.select('div.post-card-text-wrapper a.post-card-article-link')[i]['href'])
                            
                           if title == '' or news_url == '':
                                   str_n.append('Null')
                           # Исключаем повторяющиеся новости       
                           if news_url not in self.except_url():
                               str_n.append(news_time)
                               str_n.append(title)
                               str_n.append(news_url)
                           # Запись в файл уникальных новостей     
                           writer.writerow(str_n)
                           str_n = []


def main():
    
    url = 'https://www.Benzinga.com/markets'  
    news_1 = newsBenzinga(url)

    # Задаю параметры для автообновления csv таблицы
    scheduler = BackgroundScheduler()
    time = int(input("Введите через сколько секунд обнолвять таблицу: "))
    scheduler.add_job(news_1.write_file, 'interval', seconds = time)
    scheduler.start()

if __name__ == "__main__":
     while True:
         main()



import json
import re
from time import sleep

import requests
from bs4 import BeautifulSoup
from selenium.webdriver import Firefox
from selenium.webdriver.firefox.options import Options

USER_AGENT_HEADER = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.106 Safari/537.36'}

class ParserCountException(ValueError):
    """
    Ошибка определения количества просмотров страницы
    """


class PikabuParser:
    SELECTOR = '//span[@class="story__views-count"]'

    def __init__(self, timeout_s=60, sleep_after_s=3):
        opts = Options()
        opts.headless = True
        self.driver = Firefox(options=opts)
        self.driver.set_page_load_timeout(timeout_s)
        self.sleep_after_s = sleep_after_s

    @classmethod
    def get_count(cls, url):
        return cls()(url)

    def __call__(self, url):
        assert "pikabu.ru" in url
        try:
            self.driver.get(url)
            sleep(self.sleep_after_s)
            element = self.driver.find_element_by_xpath(self.SELECTOR)
        except Exception as exc:
            raise ParserCountException(str(exc))
        value_str = element.text
        if not value_str:
            raise ParserCountException("No view count on page ({})".format(value_str))
        if value_str[-1] == 'K':
            # 85.6K e.g.
            return int(float(value_str[:-1]) * 1000)
        else:
            raise NotImplementedError(
                f"Can't parse Pikabu count {value_str} from {url}"
            )


class PornHubParser:
    def __init__(self, url:str, timeout=3):
        self.headers = USER_AGENT_HEADER
        self.url = url
        self.timeout = timeout

    def get_count_views(self):
        response = requests.get(url=self.url, headers=self.headers, timeout=self.timeout)
        soup = BeautifulSoup(response.text, "html.parser")
        views = soup.find('span', class_='count')
        return int(views.text)


class HabrParser:
    def __init__(self, url:str, timeout=3):
        self.headers = USER_AGENT_HEADER
        self.url = url
        self.timeout = timeout

    def get_count_views(self):
        response = requests.get(url=self.url, headers=self.headers, timeout=self.timeout)
        soup = BeautifulSoup(response.text, "html.parser")
        views = soup.find('span', class_='post-stats__views-count')
        if 'k' in views.text:
            clean_views = views.text.replace(',', '.').replace('k', '')
            return int(float(clean_views)*1000)
        else:
            return int(views.text)


class HabrParser:
    def __init__(self, url:str, timeout=3):
        self.headers = USER_AGENT_HEADER
        self.url = url
        self.timeout = timeout

    def get_count_views(self):
        response = requests.get(url=self.url, headers=self.headers, timeout=self.timeout)
        json_regex = r'window\["ytInitialData"] = ({.*?});'
        extracted_json = re.search(json_regex, response.text).group(1)
        if 'contents' in json.loads(extracted_json).keys():
            result_json = json.loads(extracted_json)['contents']["twoColumnWatchNextResults"]['results']['results']['contents'][0]['videoPrimaryInfoRenderer']['viewCount']['videoViewCountRenderer']['viewCount']['simpleText']
            views = result_json.replace(' просмотра', '').replace(' просмотров', '').replace(' просмотр', '').replace(' views', '')
            return int(views)
        else:
            return "BROKEN VIDEO" #TODO


class RuTubeParser:
    def __init__(self, url:str, timeout=3):
        self.headers = USER_AGENT_HEADER
        self.url = url
        self.timeout = timeout

    def get_count_views(self):
        response = requests.get(url=self.url, headers=self.headers, timeout=self.timeout)
        soup = BeautifulSoup(response.text, "html.parser")
        views = soup.find('span', class_="video-info-card__view-count")
        return int(views.text.replace(',', ''))


class VimeoParser:
    def __init__(self, url:str, timeout=3):
        self.headers = USER_AGENT_HEADER
        self.url = url
        self.timeout = timeout
        self.additional_headers = {'accept': 'application/json','x-requested-with': 'XMLHttpRequest'}

    def get_count_views(self):
        self.headers.update(self.additional_headers)
        response = requests.get(url=f'{self.url}?action=load_stat_counts', headers=self.headers, timeout=self.timeout)
        if 'total_plays' in response.json().keys():
            return response.json()['total_plays']['raw']
        else:
            return 'NO VIEWS' #TODO

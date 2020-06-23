import json
import re
from time import sleep

import requests
from requests.exceptions import RequestException
from bs4 import BeautifulSoup
from selenium.webdriver import Firefox
from selenium.webdriver.firefox.options import Options
from selenium.common.exceptions import WebDriverException

USER_AGENT_HEADER = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.106 Safari/537.36'}


class PikabuParser:
    """
    Классом можно пользоваться как остальными парсерами, передавая
    в get_count_views параметра url, но тогда будет создаваться
    драйвер браузера на каждый запрос, что очень затратно.
    Поэтому можно также создать класс без параметра url и передавать
    его в методе get_count_views
    """
    SELECTOR = '//span[@class="story__views-count"]'
    DOMAIN = 'pikabu.ru'

    def __init__(self, url=None, timeout_s=60, sleep_after_s=3):
        opts = Options()
        opts.headless = True
        self.driver = Firefox(options=opts)
        self.driver.set_page_load_timeout(timeout_s)
        self.sleep_after_s = sleep_after_s
        self.url = url

    def __call__(self, url, timeout=None):
        self.url = url
        return self

    def get_count_views(self, url=None):
        if url is None and self.url is not None:
            url = self.url
        elif url is not None:
            pass
        else:
            raise ValueError("Both self.url and url are None!")
        assert "pikabu.ru" in url
        try:
            self.driver.get(url)
            sleep(self.sleep_after_s)
            element = self.driver.find_element_by_xpath(self.SELECTOR)
            value_str = element.text
            self.driver.get("")
        except Exception as exc:
            return str(exc)
        if not value_str:
            return "No view count on page ({})".format(value_str)
        if value_str[-1] == 'K':
            # 85.6K e.g.
            return int(float(value_str[:-1]) * 1000)
        else:
            raise NotImplementedError(
                f"Can't parse Pikabu count {value_str} from {url}"
            )


class PornHubParser:
    DOMAIN = 'pornhub.com'

    def __init__(self, url:str, timeout=3):
        self.headers = USER_AGENT_HEADER
        self.url = url
        self.timeout = timeout

    def get_count_views(self):
        response = requests.get(url=self.url, headers=self.headers, timeout=self.timeout)
        soup = BeautifulSoup(response.text, "html.parser")
        views = soup.find('span', class_='count')
        return int(views.text.replace(" ", ""))


class HabrParser:
    DOMAIN = 'habr.com'

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


class YoutubeParser:
    DOMAIN = 'youtube.com'

    def __init__(self, url:str, timeout=3):
        self.headers = USER_AGENT_HEADER
        self.url = url
        self.timeout = timeout

    def get_count_views(self):
        response = requests.get(url=self.url, headers=self.headers, timeout=self.timeout)
        json_regex = r'window\["ytInitialData"] = ({.*?});'
        extracted_json = re.search(json_regex, response.text).group(1)
        if 'contents' in json.loads(extracted_json).keys():
            try:
                result_json = json.loads(extracted_json)['contents']["twoColumnWatchNextResults"]['results']['results']['contents'][0]['videoPrimaryInfoRenderer']['viewCount']['videoViewCountRenderer']['viewCount']['simpleText']
                #views = result_json.replace(' просмотра', '').replace(' просмотров', '').replace(' просмотр', '').replace(' views', '')
                views = result_json.split(' ')[0].replace('\xa0', '')
                return int(views)
            except Exception as exc:
                return "Failed to parse count"
        else:
            return "BROKEN VIDEO" #TODO


class RuTubeParser:
    DOMAIN = 'rutube.ru'

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
    DOMAIN = "vimeo.com"

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


class UniversalViewCounter:
    """
    Обертка над всем парсерами
    """
    def __init__(self, timeout=3, use_selenium=False):
        self.timeout = timeout
        self.use_selenium = use_selenium
        self.parsers_callables = [
            VimeoParser,
            RuTubeParser,
            YoutubeParser,
            HabrParser,
            PornHubParser,
        ]
        if self.use_selenium:
            self.pikabu_parser = PikabuParser()
            # через call поддерживает тот же интерфейс что у остальных
            # парсеров, но драйвер создается один раз
            self.parsers_callables.append(self.pikabu_parser)


    def get_count_views_message(self, url):
        selected_parser = None
        for pc in self.parsers_callables:
            if pc.DOMAIN in url:
                selected_parser = pc
        if selected_parser is None:
            return f"Domain from url {url} is not supported"
        try:
            return selected_parser(url, self.timeout).get_count_views()
        except (
            RequestException, WebDriverException
        ) as exc:
            return f"Failed to fetch: {exc}"
        except Exception as exc:
            return f"Failed because of bug: {exc}"

    def quit(self):
        """
        Костыли!
        Без этого у selenium+firefox memory leak
        """
        if self.use_selenium:
            self.pikabu_parser.driver.quit()

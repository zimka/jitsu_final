import json
import re
from time import sleep

import requests
from bs4 import BeautifulSoup
from requests.exceptions import RequestException
from selenium import webdriver
from selenium.common.exceptions import WebDriverException

USER_AGENT_HEADER = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.106 Safari/537.36"
}


class PikabuParser:
    """
    Классом можно пользоваться как остальными парсерами, передавая
    в get_count_views параметра url, но тогда будет создаваться
    драйвер браузера на каждый запрос, что очень затратно.
    Поэтому можно также создать класс без параметра url и передавать
    его в методе get_count_views
    """

    SELECTOR = '//span[@class="story__views-count"]'
    DOMAIN = "pikabu.ru"

    def __init__(self, url=None, timeout_s=20, sleep_after_s=3):
        options = webdriver.ChromeOptions()
        options.add_argument("start-maximized")
        options.add_argument("enable-automation")
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-infobars")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-browser-side-navigation")
        options.add_argument("--dns-prefetch-disable")
        options.add_argument("--disable-gpu")
        # options.add_argument('--no-proxy-server')
        options.add_argument("--proxy-server='direct://'")
        options.add_argument("--proxy-bypass-list=*")
        options.add_argument(
            "user-agent=Mozilla/5.0 (X11; CrOS x86_64 8172.45.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.64 Safari/537.36"
        )
        self.driver = webdriver.Chrome(options=options)
        # self.driver.implicitly_wait(timeout_s)
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
            # sleep(self.sleep_after_s)
            generated_html = self.driver.page_source
            soup = BeautifulSoup(generated_html, "html.parser")
            watchers_tag = soup.find("div", class_="story__views hint")
            self.driver.quit()
            if watchers_tag:
                value_str = "".join(watchers_tag["aria-label"].split()[:-1])
                return int(value_str)
            else:
                return "NO VIEWS"
        except Exception as exc:
            self.driver.quit()
            print(str(exc))
            return "PARSER ERROR"


class PikabuRatingParser:
    DOMAIN = "pikabu.ru"

    def __init__(self, url: str, timeout=3):
        self.headers = USER_AGENT_HEADER
        self.url = url
        self.timeout = timeout

    def get_count_views(self):
        try:
            response = requests.get(url=self.url, headers=self.headers, timeout=self.timeout)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
            rating = soup.find("div", class_="story__rating-count")
            if rating:
                return int(rating.text)
            else:
                return "PARSER ERROR"
        except (requests.RequestException, ValueError):
            return "BAD URL"


class PornHubParser:
    DOMAIN = "rt.pornhub.com"

    def __init__(self, url: str, timeout=3):
        self.headers = USER_AGENT_HEADER
        self.url = url
        self.timeout = timeout

    def get_count_views(self):
        try:
            response = requests.get(url=self.url, headers=self.headers, timeout=self.timeout)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
            views = soup.find("span", class_="count")
            if views:
                return int(views.text.replace(" ", ""))
            else:
                return "NO VIEWS"
        except (requests.RequestException, ValueError):
            return "BAD URL"


class HabrParser:
    DOMAIN = "habr.com"

    def __init__(self, url: str, timeout=3):
        self.headers = USER_AGENT_HEADER
        self.url = url
        self.timeout = timeout

    def get_count_views(self):
        try:
            response = requests.get(url=self.url, headers=self.headers, timeout=self.timeout)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
            views = soup.find("span", class_="post-stats__views-count")
            if "k" in views.text:
                clean_views = views.text.replace(",", ".").replace("k", "")
                return int(float(clean_views) * 1000)
            else:
                return int(views.text)
        except (requests.RequestException, ValueError):
            return "BAD URL"


class YoutubeParser:
    DOMAIN = "youtube.com"

    def __init__(self, url: str, timeout=3):
        self.headers = USER_AGENT_HEADER
        self.url = url
        self.timeout = timeout

    def get_count_views(self):
        try:
            response = requests.get(url=self.url, headers=self.headers, timeout=self.timeout)
            response.raise_for_status()
            json_regex = r'window\["ytInitialData"] = ({.*?});'
            extracted_json = re.search(json_regex, response.text).group(1)
            if "contents" in json.loads(extracted_json).keys():
                result_json = json.loads(extracted_json)["contents"]["twoColumnWatchNextResults"][
                    "results"
                ]["results"]["contents"][0]["videoPrimaryInfoRenderer"]["viewCount"][
                    "videoViewCountRenderer"
                ][
                    "viewCount"
                ][
                    "simpleText"
                ]
                views = result_json.split(" ")[0].replace("\xa0", "").replace(",", "")
                return int(views)
            else:
                return "BROKEN VIDEO"
        except (requests.RequestException, ValueError):
            return "BAD URL"


class RuTubeParser:
    DOMAIN = "rutube.ru"

    def __init__(self, url: str, timeout=3):
        self.headers = USER_AGENT_HEADER
        self.url = url
        self.timeout = timeout

    def get_count_views(self):
        try:
            response = requests.get(url=self.url, headers=self.headers, timeout=self.timeout)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
            views = soup.find("span", class_="video-info-card__view-count")
            if views:
                return int(views.text.replace(",", ""))
            else:
                return "NO VIEWS"
        except (requests.RequestException, ValueError):
            return "BAD URL"


class VimeoParser:
    DOMAIN = "vimeo.com"

    def __init__(self, url: str, timeout=3):
        self.headers = USER_AGENT_HEADER
        self.url = url
        self.timeout = timeout
        self.additional_headers = {
            "accept": "application/json",
            "x-requested-with": "XMLHttpRequest",
        }

    def get_count_views(self):
        try:
            self.headers.update(self.additional_headers)
            response = requests.get(
                url=f"{self.url}?action=load_stat_counts",
                headers=self.headers,
                timeout=self.timeout,
            )
            if response.status_code == 403:
                return "PARSER ERROR"
            else:
                response.raise_for_status()
                if "total_plays" in response.json().keys():
                    return response.json()["total_plays"]["raw"]
                else:
                    return "NO VIEWS"
        except (requests.RequestException, ValueError):
            return "BAD URL"


class UniversalViewCounter:
    """
    Обертка над всем парсерами
    """

    def __init__(self, timeout=3, pikabu_rating=False, use_selenium=True):
        self.timeout = timeout
        self.pikabu_rating = pikabu_rating
        self.use_selenium = use_selenium
        self.parsers_callables = [
            VimeoParser,
            RuTubeParser,
            YoutubeParser,
            HabrParser,
            PornHubParser,
        ]
        if self.pikabu_rating:
            # загушка вместо селениума
            self.parsers_callables.append(PikabuRatingParser)
        if self.use_selenium:
            self.pikabu_parser = PikabuParser()
            # через call поддерживает тот же интерфейс что у остальных
            # парсеров, но драйвер создается один раз
            self.parsers_callables.append(self.pikabu_parser)

    def get_count_views_message(self, url: str):
        selected_parser = None
        for pc in self.parsers_callables:
            if pc.DOMAIN in url:
                selected_parser = pc
        if selected_parser is None:
            return "BAD DOMAIN"
        return selected_parser(url, self.timeout).get_count_views()

    def quit(self):
        """
        Костыли!
        Без этого у selenium+firefox memory leak
        """
        if self.use_selenium:
            self.pikabu_parser.driver.quit()

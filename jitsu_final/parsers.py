import requests
from time import sleep
from selenium.webdriver import Firefox
from selenium.webdriver.firefox.options import Options


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


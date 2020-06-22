Установка
---------

::

  pip install -e git+https://github.com/zimka/jitsu_final#egg=jitsu_final


Тест парсера
------------
::

    $ python
    from jitsu_final.parsers import PikabuParser
    PikabuParser.get_count('https://pikabu.ru/story/schetchik_prosmotrov__skolko_lyudey_uvideli_post_7220783')


Тест пайплайна без airflow
--------------------------

1. Развернуть в докере/локально/в облаке базу, например
::

  sudo docker run -p 5432:5432 -e POSTGRES_PASSWORD=0 postgres:11.0

2. $ python
::

    from jitsu_final import run_view_count
    run_view_count('postgresql+psycopg2://postgres:0@localhost:5432/postgres', '', {})


Запуск тестов
-------------

$ cd jitsu_final
$ pytest tests.py

import ujson as json # для распаковки/упаковки объектов (прокачанная версия)
import time # для слипов
from selenium import webdriver # для открытия сайта целевого в браузере в headless режиме
from selenium.common.exceptions import TimeoutException # для отслеживания таймаута веб-драйвера
import requests # для получения данных с url-источников
from datetime import datetime # для перевода timestamp-меток в нормальный формат
import pandas as pd # для удобного преобразования в xlsx

# класс парсера целевого сайта
class URL_Parser():
    # функция инициализации класса парсера
    def __init__(self, parent_url:str, delay:int, url_pattern:str, logger:object, connection_timeout:int) -> None:
        self._parent_url = parent_url # ссылка на первоначальный сайт
        self._url_pattern = url_pattern # что должен содержать url, чтобы подходить под шаблон целевого
        self._delay = delay # временная задержка
        self._logger = logger 
        self._competitions_all = []
        self._connection_timeout = connection_timeout
        return 
    
    # получает все запросы сайта к внешним ресурсам
    def _get_live_urls(self) -> list:
        # инициализация браузера
        options = webdriver.ChromeOptions()
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--headless=new")
        options.set_capability("goog:loggingPrefs", {"performance": "ALL"})
        driver = webdriver.Chrome(options=options)
        driver.set_page_load_timeout(self._connection_timeout) # таймаут именно для загрузки страницы
        try:
            # отлавливание запросов
            try:
                driver.execute_cdp_cmd("Network.enable", {})
                driver.get(self._parent_url)
            except TimeoutException:
                self._logger.error("Timeout error while connecting to host")
                return []
            self._logger.info(f"Wait {self._delay} sec")
            # та самая задержка 
            time.sleep(self._delay)
            logs = driver.get_log("performance")
            urls = set() # создание сета для хранения уникальных значений
            for entry in logs:
                msg = json.loads(entry["message"])["message"]
                if msg.get("method") != "Network.requestWillBeSent": continue
                req = msg.get("params", {}).get("request", {})
                url = req.get("url")
                if url: urls.add(url)

            # проверка соответствия элемента шаблону
            score_urls = []
            for url in urls:
                if self._url_pattern in url: score_urls.append(url)
        except Exception as e:
            self._logger.error(f"Error while connecting remote host")
            return []
        finally:
            driver.quit()        
        return score_urls
    
    def _is_this_event(self, event_id:int) -> bool:
        for event in self._competitions_all:
            if event_id == event["id"]: 
                return True 
        return False
    
    # вытасиквает данные с переданного url
    def get_data_from_url(self, url:str) -> None:
        url_data = {}
        try:
            url_data = requests.get(url, timeout=self._connection_timeout).json() # получает пакет данных
        except requests.Timeout: # обработка ошибки долгого ожидания
            self._logger.error("Time for request is out. Bad connection") 
            return None
        except requests.RequestException as e: # обработка ошибка bad request
            self._logger.error(f"Error while connecting with remote host. Bad connection, {e}")
            return None

        # проверка на наличие поля
        if "sports" in url_data.keys():
            sport_keys = []
            for key in url_data["sports"]:
                if "esoccer" in url_data["sports"][key]["name"].lower(): # берем только спорт-категории с присутствующим словом eSoccer
                    sport_keys.append(key)
            for event_id in url_data["events"].keys(): # в events хранится информация о матчах
                event = url_data["events"][event_id]
                competition = { # итоговый объект для сохранения в БД
                    "time": datetime.now(),
                    "id": event_id, # айди матча
                    "scheduled": None, # время старта
                    "player_1": None, # игрок 1
                    "player_2": None, # игрок 2
                    "score_per_1_home": -1, # голов у первой команды после 1 тайма
                    "score_per_1_away": -1, # голов у второй команды после 1 тайма
                    "res_score_home": -1, # голов у первой команды после игры
                    "res_score_away": -1, # голов у второй команды после игры
                }
                if event and ("desc" in event.keys()) and (event["desc"]["sport"] in sport_keys): # в desc хранится описание матча - кто играет и на какое время запланировано
                    competition["scheduled"] = datetime.fromtimestamp(event["desc"]["scheduled"]).strftime("%Y-%m-%d %H:%M:%S")
                    competition['player_1'] = event["desc"]["competitors"][0]["name"]
                    competition["player_2"] = event["desc"]["competitors"][1]["name"]
                    if not self._is_this_event(event_id=event_id):
                        self._competitions_all.append(competition)
                
                # обработка голов
                if event and ("score" in event.keys()) and (self._is_this_event(event_id=event_id)):
                    for event_index, event_competition in enumerate(self._competitions_all):
                        if event_id == event_competition["id"]:
                            if len(event["score"]["period_scores"]) > 0: 
                                self._competitions_all[event_index]["score_per_1_home"] = event["score"]["period_scores"][0]["home_score"]
                                self._competitions_all[event_index]["score_per_1_away"] = event["score"]["period_scores"][0]["away_score"]
                            self._competitions_all[event_index]["res_score_home"] = int(event["score"]["home_score"])
                            self._competitions_all[event_index]["res_score_away"] = int(event["score"]["away_score"])
        return None
    
    # записывает собранные данные в файл
    def write_log(self, data:list) -> None:
        self._logger.warning("Log saving is started. Don't close the program")
        log_dict = {
            "time": [],
            "id": [],
            "scheduled": [],
            "player_1": [],
            "player_2": [],
            "score_per_1_home": [],
            "score_per_1_away":[],
            "res_score_home": [],
            "res_score_away": []
        }
        for d in data:
            for key in d.keys():
                log_dict[key].append(d[key])
        df = pd.DataFrame(log_dict)
        df["time"] = pd.to_datetime(df["time"])
        df["scheduled"] = pd.to_datetime(df["scheduled"])
        df = df.sort_values("scheduled") # сортировка по времени старта
        with pd.ExcelWriter(f"./res_logs/parser_results.xlsx", engine="xlsxwriter") as writer:
            df.to_excel(writer, sheet_name="sheet_1", index=False)
            worksheet = writer.sheets["sheet_1"]
            # форматирование таблицы по визуалу
            for i, col in enumerate(df.columns):
                max_len = max(df[col].astype(str).map(len).max(), len(col))
                worksheet.set_column(i, i, max_len + 2)
        self._logger.info("Log is saved into ./res_logs/parser_results.xlsx")
        return
    
    # главная точка входа в класс
    def main(self) -> None:
        self._logger.info("Parser is started")
        urls = self._get_live_urls()
        for u_index, url in enumerate(urls):
            self._logger.info(f"Doing {u_index}/{len(urls)-1}")
            self.get_data_from_url(url=url)
        
        self.write_log(data=self._competitions_all)
        return

# точка входа в программу
if __name__ == "__main__":
    parser = URL_Parser()
    parser.main()
    
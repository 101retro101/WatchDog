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
        self._log_is_start = False

        # один webdriver на весь объект
        options = webdriver.ChromeOptions()
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--headless=new")
        options.set_capability("goog:loggingPrefs", {"performance": "ALL"})
        self._driver = webdriver.Chrome(options=options)
        self._driver.set_page_load_timeout(self._connection_timeout)
        return 
    
    # получает все запросы сайта к внешним ресурсам
    def _get_live_urls(self) -> list:
        try:
            # отлавливание запросов
            try:
                self._driver.execute_cdp_cmd("Network.enable", {})
                self._driver.get(self._parent_url)
            except TimeoutException:
                self._logger.error("Timeout error while connecting to host")
                return []
            self._logger.info(f"Wait {self._delay} sec")
            # та самая задержка 
            time.sleep(self._delay)
            logs = self._driver.get_log("performance")
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
            self._logger.error(f"Error while connecting remote host, {e}")
            return []       
        return score_urls
    
    def _is_this_event(self, event_id:int) -> int | None:
        for event_index, event in enumerate(self._competitions_all):
            if event_id == event["id"]: 
                return event_index 
        return None
    
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
                is_this_event = self._is_this_event(event_id=event_id)
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
                    if is_this_event:
                        pass
                    else:
                        self._competitions_all.append(competition)
                
                # обработка голов
                if event and ("score" in event.keys()) and (is_this_event):
                    if len(event["score"]["period_scores"]) > 0: 
                        self._competitions_all[is_this_event]["score_per_1_home"] = event["score"]["period_scores"][0]["home_score"]
                        self._competitions_all[is_this_event]["score_per_1_away"] = event["score"]["period_scores"][0]["away_score"]
                    self._competitions_all[is_this_event]["res_score_home"] = int(event["score"]["home_score"])
                    self._competitions_all[is_this_event]["res_score_away"] = int(event["score"]["away_score"])    
        return None
    
    def write_log(self,  data:list) -> None:
        self._logger.warning("Log saving is started. Don't close the program")
        
        csv_path = "./res_logs/parser_results.csv"
        headers = ["time", "id", "scheduled", "player_1", "player_2", "score_per_1_home", "score_per_1_away", "res_score_home", "res_score_away"]
        
        # пишем батчами по 1000 строк
        batch_size = 1000
        batches = [data[i:i + batch_size] for i in range(0, len(data), batch_size)]
        
        for batch in batches:
            # создаём маленький DataFrame только для батча
            log_dict = {h: [d.get(h) for d in batch] for h in headers}
            df_batch = pd.DataFrame(log_dict)

            # конвертируем datetime только для батча
            df_batch["time"] = pd.to_datetime(df_batch["time"])
            df_batch["scheduled"] = pd.to_datetime(df_batch["scheduled"])
            df_batch = df_batch.sort_values("scheduled")
            
            # append в CSV с точкой с запятой
            df_batch.to_csv(csv_path, sep=";", index=False, mode='w', header=(not pd.io.common.file_exists(csv_path)), encoding="utf-8-sig")
        
        self._logger.info(f"Log saved to {csv_path} ({len(data)} rows)")
    
    # главная точка входа в класс
    def main(self) -> None:
        self._logger.info("Parser iteration is started")
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
    
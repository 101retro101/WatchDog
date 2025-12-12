import ujson as json # для распаковки/упаковки объектов (прокачанная версия)
import time # для слипов
from selenium import webdriver # для открытия сайта целевого в браузере в headless режиме
from selenium.common.exceptions import TimeoutException # для отслеживания таймаута веб-драйвера
import requests # для получения данных с url-источников
from datetime import datetime # для перевода timestamp-меток в нормальный формат
import pandas as pd # для удобного преобразования в xlsx

# обновляет старый элемент с учетом нового
def update_elem(elem_1:dict, elem_2:dict) -> dict:
    new_elem = elem_1
    coef_keys = ["coef_1", "coef_2", "coef_3"] 
    score_keys = ["score_per_1_home", "score_per_1_away", "res_score_home", "res_score_away"]
    for key in coef_keys:
        if not(elem_2[key] == None):
            new_elem[key] = elem_2[key]
    for key in score_keys:
        if not(elem_2[key] == 0) and (elem_1[key] <= elem_2[key]):
            new_elem[key] = elem_2[key]
    date_1 = datetime.strptime(elem_1["scheduled"], "%Y-%m-%d %H:%M:%S")
    date_2 = datetime.strptime(elem_2["scheduled"], "%Y-%m-%d %H:%M:%S")
    if date_1 < date_2:
        new_elem["scheduled"] = elem_1["scheduled"]
    else:
        new_elem["scheduled"] = elem_2["scheduled"]
    return new_elem

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
    
    # вытасиквает данные с переданного url
    def get_data_from_url(self, url:str) -> None or list:
        url_data = {}
        try:
            url_data = requests.get(url, timeout=self._connection_timeout).json() # получает пакет данных
        except requests.Timeout: # обработка ошибки долгого ожидания
            self._logger.error("Time for request is out. Bad connection") 
            return None
        except requests.RequestException as e: # обработка ошибка bad request
            self._logger.error(f"Error while connecting with remote host. Bad connection, {e}")
            return None
        competitions = []
        # проверка на наличие поля
        if "sports" in url_data.keys():
            sport_keys = []
            for key in url_data["sports"]:
                if "esoccer" in url_data["sports"][key]["name"].lower(): # берем только спорт-категории с присутствующим словом eSoccer
                    sport_keys.append(key)
            for event_id in url_data["events"].keys(): # в events хранится информация о матчах
                event = url_data["events"][event_id]
                if event and "desc" in event.keys(): # в dedsc хранится описание матча - кто играет и на какое время запланировано
                    if event["desc"]["sport"] in sport_keys:
                        competition = { # итоговый объект для сохранения в БД
                            "time": datetime.now(),
                            "id": event_id, # айди матча
                            "scheduled": datetime.fromtimestamp(event["desc"]["scheduled"]).strftime("%Y-%m-%d %H:%M:%S"), # время старта
                            "player_1": event["desc"]["competitors"][0]["name"], # игрок 1
                            "player_2": event["desc"]["competitors"][1]["name"], # игрок 2
                            "score_per_1_home": 0, # голов у первой команды после 1 тайма
                            "score_per_1_away": 0, # голов у второй команды после 1 тайма
                            "res_score_home": 0, # голов у первой команды после игры
                            "res_score_away": 0, # голов у второй команды после игры
                            "coef_1": None, # коэф с сайта на победу первой команды
                            "coef_2": None, # коэф с сайта на ничью
                            "coef_3": None, # коэф с сайта на победу второй команды
                        }
                        # обработка голов
                        if "score" in event.keys():
                            if len(event["score"]["period_scores"]) > 0: 
                                competition["score_per_1_home"] = event["score"]["period_scores"][0]["home_score"]
                                competition[f"score_per_1_away"] = event["score"]["period_scores"][0]["away_score"]
                            competition["res_score_home"] = int(event["score"]["home_score"])
                            competition["res_score_away"] = int(event["score"]["away_score"])
                        # обработка коэфов
                        if "markets" in event.keys() and "1" in event["markets"].keys() and event["markets"]["1"]:
                            competition["coef_1"] = float(event["markets"]["1"][""]["1"]["k"])
                            competition["coef_2"] = float(event["markets"]["1"][""]["2"]["k"])
                            competition["coef_3"] = float(event["markets"]["1"][""]["3"]["k"])
                        competitions.append(competition)

            # так как в пакете поступают данные и с видов спорта, не нужных нам, то может произойти, что в пакете вообще не будет данных о eSoccer
            if len(competitions) == 0: 
                return None 
            else: 
                return competitions
        else: 
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
            "res_score_away": [],
            "coef_1": [],
            "coef_2": [],
            "coef_3": []
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
            c = self.get_data_from_url(url=url)
            if c:
                self._competitions_all += c
        
        #отсеивание дубликатов (обновление старых элементов новыми)
        by_id = {}
        for c in self._competitions_all:
            scores = ["score_per_1_home","score_per_1_away", "res_score_home", "res_score_away"]
            score_summ = 0
            for key in scores:
                score_summ += c[key]
            if score_summ == 0: continue
            c_id = c["id"]
            if c_id in by_id.keys(): 
                new_elem = update_elem(elem_1=by_id[c_id], elem_2=c)
                by_id[c_id] = new_elem
            else:
                by_id[c_id] = c
        unique = list(by_id.values())

        self._competitions_all = unique
        self.write_log(data=self._competitions_all)
        return

# точка входа в программу
if __name__ == "__main__":
    parser = URL_Parser()
    parser.main()
    
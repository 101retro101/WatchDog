import ujson as json
from modules.parser import URL_Parser
from modules.logger import Logger

# получение конфигурационного файла
def get_config() -> dict:
    with open("./config.json") as config:
        config = json.loads(config.read())
    return config

# главная функция программы
def main() -> None:
    config = get_config()
    # для логгирования событий
    logger = Logger(
        logger_name="watchdog",
        log_to_file=config["log_to_file"]
    ).logger
    # сам экземпляр класса парсера
    parser = URL_Parser(
        parent_url=config["parent_url"],
        delay=config["delay"],
        url_pattern=config["url_pattern"],
        logger=logger,
        connection_timeout=config["connection_timeout"]
    )
    # чтобы парсер работал бесконечно
    while True:
        parser.main()
    return 

# точка входа в главную программу
if __name__ == "__main__":
    try:
        main()
    # чтобы вырубить парсер надо нажать CTRL+C
    except KeyboardInterrupt as e:
        print("Exit Program")
import pandas as pd # для удобной работы с логами
import ujson as json # для упаковки/распаковки объектов
from pathlib import Path # для проверки путей
from modules.logger import Logger

# используется одна из функций pd, из старых версий
import warnings
warnings.filterwarnings('ignore', category=FutureWarning)

# получение конфига
def get_config() -> dict:
    with open("./config.json") as config:
        config = json.loads(config.read())
    return config

# получение общего датасета из файла
def get_data_from_excel(path:Path, logger) -> pd.DataFrame or None:
    try:
        df = pd.read_excel(path)
        return df 
    except Exception as e:
        logger.error(f"Error while openning excel database, {e}")
        return None
    
# подсчет кто является победителем в матче
def winner(row:pd.DataFrame) -> str:
    if row['res_score_home'] > row['res_score_away']:
        return row['player_1']
    elif row['res_score_home'] < row['res_score_away']:
        return row['player_2']
    else:
        return 'draw'
    
# подсчет строчки: команда_1, команда_2, кол_во_побед_1, кол_во_побед_2, кол_во_ничьих
def count_wins(group:pd.DataFrame) -> pd.Series:
    team1, team2 = group.name
    wins_team_1 = ((group['winner'] == team1)).sum()
    wins_team_2 = ((group['winner'] == team2)).sum()
    draws = ((group['winner'] == 'draw')).sum()
    return pd.Series({'team1': team1, 'team2': team2, 'wins_team_1': wins_team_1, 'wins_team_2': wins_team_2, 'draws': draws})

# основной класс
class Log_stat():
    def __init__(self):
        self._config = get_config()
        self._logger = Logger(
            logger_name="log_stat",
            log_to_file=self._config["log_to_file"]
        ).logger

    # получение пути к файлу лога
    def _get_file_path(self) -> Path or None:
        file_path = Path(input("Enter file path: "))
        if file_path.exists():
            return file_path
        else:
            self._logger.error("Path doesn't exist")
            return None
    
    # расчет статистики
    def _stat_df(self, df:pd.DataFrame) -> None:
        df["team_group"] = df.apply(lambda row: tuple(sorted([row['player_1'], row['player_2']])), axis=1)
        df["winner"] = df.apply(winner, axis=1)
        group_df = df.groupby('team_group')
        counter_df = group_df.apply(count_wins).reset_index(drop=True)
        counter_df.to_excel("./test.xlsx")
        # group_df - объект с группированными строчками по командам, то есть в первой группе
        # все матчи между командой_1 и командой_2 условно и т.д.

        # counter_df - объект с рассчитанными победами и ничьими для каждой группы 
        # в каждой строчке: команда_1, команда_2, сколько раз выиграла 1 команда, сколько раз выиграла 2 команда, сколько раз была ничья

        # пример обработки каждой группы
        for teams, group in group_df:
            print(f"Матчи между командами: {teams}")
            for g_i, g_r in group.iterrows():
                print(g_r)
            print("Количество матчей в группе:", len(group))
            print("-"*20)
        return

    # точка входа в исполнение класса
    def main(self) -> None:
        file_path = self._get_file_path()
        if file_path:
            df = get_data_from_excel(path=file_path, logger=self._logger)
            if not df.empty:
                self._stat_df(df=df)        
            else:
                return
        else:
            return

# точка входа в программу
if __name__ == "__main__":
    stat = Log_stat()
    stat.main()

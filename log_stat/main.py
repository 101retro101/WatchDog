import pandas as pd # для удобной работы с логами
import ujson as json # для упаковки/распаковки объектов
from pathlib import Path # для проверки путей
from modules.logger import Logger
import os # для путей логов

# используется одна из функций pd, из старых версий
import warnings
warnings.filterwarnings('ignore', category=FutureWarning)

def get_config() -> dict:
    '''
    получает конфигурацию программы
    :return: словарь конфигурации
    '''
    with open("./config.json") as config:
        config = json.loads(config.read())
    return config

def get_data(path:str) -> pd.DataFrame | None:
    '''  
    получает всю информацию из папки с логами и объединяет ее в один объект
    :param path: путь к папке с логами
    :return: датафрейм или ничего
    '''
    header = ["time", "id", "scheduled", "player_1", "player_2", "home_score_per_1", "away_score_per_1", "home_score", "away_score"]
    main_df = pd.DataFrame(columns=header)
    ids = []
    for file in os.listdir(path):
        file_path = path + file 
        df = pd.read_csv(file_path, delimiter=";", header=None, names=header)
        for _, row in df.iterrows():
            if row["id"] not in ids: # создание новой строчки
                main_df = pd.concat([main_df, row.to_frame().T], ignore_index=True)
                ids.append(row["id"])
            else: # обновление существующей
                old_row = main_df[main_df['id'] == row["id"]].iloc[0]
                old_row_index = main_df[main_df['id'] == row["id"]].index[0]
                keys = ["home_score_per_1", "away_score_per_1", "home_score", "away_score"]
                for k in keys:
                    if old_row[k] < row[k]:
                        main_df.loc[old_row_index, k] = row[k]
    return main_df
    
def winner(row:pd.DataFrame) -> str:
    '''  
    определяет кто выиграл в матче
    :param row: датафрейм с матчем
    :return: имя победившей команды или draw (ничья)
    '''
    if row['home_score'] > row['away_score']:
        return row['player_1']
    elif row['home_score'] < row['away_score']:
        return row['player_2']
    else:
        return 'draw'
    
def count_wins(group:pd.DataFrame) -> pd.Series:
    '''  
    считает кол-во подеб в рамках группы (одни и те же участники)
    :param group: группа матчей с одиними и теми же участниками
    :return: датафрейм с подсчитанными победами и ничьими
    '''
    team1, team2 = group.name
    wins_team_1 = ((group['winner'] == team1)).sum()
    wins_team_2 = ((group['winner'] == team2)).sum()
    draws = ((group['winner'] == 'draw')).sum()
    all = wins_team_1 + wins_team_2 + draws
    return pd.Series({'team1': team1, 'team2': team2, 'all': all, 'wins_team_1': wins_team_1, 'wins_team_2': wins_team_2, 'draws': draws})

class Log_stat():
    '''  
    главный класс подсчета статистики
    '''
    def __init__(self):
        self._config = get_config()
        self._logger = Logger(
            logger_name="log_stat",
            log_to_file=self._config["log_to_file"]
        ).logger
    
    def _stat_df(self, df:pd.DataFrame) -> None:
        '''  
        считает статистику по всему файлу бд
        '''
        df["team_group"] = df.apply(lambda row: tuple(sorted([row['player_1'], row['player_2']])), axis=1)
        df["winner"] = df.apply(winner, axis=1)
        df["id"] = df["id"].astype(str)
        df.to_excel("merge.xlsx")
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

    def main(self) -> None:
        df = get_data(path=self._config["path_to_logs"])
        if not df.empty:
            self._stat_df(df=df)        
        else:
            return

# точка входа в программу
if __name__ == "__main__":
    stat = Log_stat()
    stat.main()

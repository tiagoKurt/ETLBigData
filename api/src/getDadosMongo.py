import os
import time

import numpy as np
import pandas as pd
from connectMongo import ConnectMongo
from exchange import QueueExchange


class GetDadosMongo:
    def __init__(self, colecao, database):
        self.conn = ConnectMongo().connect()
        self.colecao = colecao
        self.database = database

    def getData(self):
        caminho = "/app/parquet/dados.parquet"

        if os.path.isfile(caminho):  # Verifica se o arquivo existe
            df = pd.read_parquet(caminho)

            # Substitui valores problemáticos antes de converter para JSON
            df.replace(
                [np.inf, -np.inf], np.nan, inplace=True
            )  # Substitui infinitos por NaN
            df.fillna(0, inplace=True)  # Substitui NaN por 0 (ou outro valor adequado)

            return df.to_dict(orient="records")

        exchange.sendMsg(
            {"colecao": self.colecao, "database": self.database, "status": False},
            rkey="etlmongodb-verificador_service_tiago",
        )

        # Aguarda até que o arquivo apareça
        while True:
            if os.path.isfile(caminho):  # Verifica novamente se o arquivo foi criado
                df = pd.read_parquet(caminho)

                # Substitui valores problemáticos antes de converter para JSON
                df.replace(
                    [np.inf, -np.inf], np.nan, inplace=True
                )  # Substitui infinitos por NaN
                df.fillna(
                    0, inplace=True
                )  # Substitui NaN por 0 (ou outro valor adequado)

                return df.to_dict(orient="records")
            else:
                print("DATABASE IS NOT HERE!!!!!", flush=True)
                time.sleep(5)  # Aguarda 5 segundos antes de tentar novamente


exchange = QueueExchange(
    host="rabbitmq",
    service="etlmongodb-api",
    client="tiago",
)
exchange.start_consuming()

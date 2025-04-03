import json

import pandas as pd
from connectMongo import ConnectMongo
from exchange import QueueExchange


class GetDataFromMongo:
    def __init__(self, colecao, database):
        self.conn = ConnectMongo().connect()
        self.colecao = colecao
        self.database = database

    def transform_data(self):
        database = self.conn[self.database]
        collection = database[self.colecao]
        df = pd.DataFrame(list(collection.find()))

        df[["Altura", "Largura", "Comprimento"]] = df["dimensaoProduto"].str.extract(
            r"(\d+)\s*x\s*(\d+)\s*x\s*(\d+)"
        )

        df[["Altura", "Largura", "Comprimento"]] = df[
            ["Altura", "Largura", "Comprimento"]
        ].astype(float)

        # Verificar se o valor contém "/"
        df["cor"] = df["variacoesDeCorETamanho"].apply(
            lambda x: x.split("/")[0] if isinstance(x, str) and "/" in x else x
        )
        df["tamanho"] = df["variacoesDeCorETamanho"].apply(
            lambda x: x.split("/")[1] if isinstance(x, str) and "/" in x else None
        )
        df["unidadeMedida"] = "cm"

        # Converter para datetime
        df[["dataProducao", "dataExpiracao"]] = df[
            ["dataProducao", "dataExpiracao"]
        ].apply(pd.to_datetime)

        # Formatar as datas corretamente
        df["dataProducao"] = df["dataProducao"].dt.strftime("%d/%m/%Y")
        df["dataExpiracao"] = df["dataExpiracao"].dt.strftime("%d/%m/%Y")
        df_agrupado = df.groupby(["nome", "categoriaProduto"], as_index=False).size()

        # Renomear a coluna 'size' para 'vendas'
        df_agrupado.rename(columns={"size": "vendas"}, inplace=True)

        # Calcular vendas totais por categoria e adicionar ao DF original
        df["vendasTotais"] = df["categoriaProduto"].map(
            df_agrupado.groupby("categoriaProduto")["vendas"].sum()
        )

        # Substituir valores NaN por 0
        df.fillna(0, inplace=True)

        # Garantir que 'vendasTotais' é inteiro
        df["vendasTotais"] = df["vendasTotais"].astype(int)
        # Converter a coluna _id para string (se existir)
        if "_id" in df.columns:
            df["_id"] = df["_id"].apply(
                lambda x: str(x) if isinstance(x, object) else x
            )

        # Agora, salvar como Parquet
        df.to_parquet("/parquet/dados.parquet", engine="pyarrow", index=False)


def callback(ch, method, properties, body):
    print("SERVICE TRANSFORMARDADOS RUNNING", flush=True)
    dado = body.decode("utf-8")
    print(f" [x] Received {dado}", flush=True)
    dado = json.loads(dado)
    if dado["status"]:
        transform = GetDataFromMongo(dado["colecao"], dado["database"])
        transform.transform_data()


exchange = QueueExchange(
    host="rabbitmq",
    service="etlmongodb-transformardados",
    rabbit_callback=callback,
    client="tiago",
)
exchange.start_consuming()

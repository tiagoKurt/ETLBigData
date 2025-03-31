import pandas as pd

from connectMongo import ConnectMongo


class GetDataFromMongo:
    def __init__(self):
        self.conn = ConnectMongo().connect()

    def transform_data(self):
        database = self.conn["loja"]
        collection = database["produtos"]
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

        # Converter a coluna _id para string (se existir)
        if "_id" in df.columns:
            df["_id"] = df["_id"].apply(
                lambda x: str(x) if isinstance(x, object) else x
            )

        # Agora, salvar como Parquet
        df.to_parquet("dados.parquet", engine="pyarrow", index=False)

    def get_data(self):
        self.transform_data()
        return pd.read_parquet("dados.parquet").to_dict(orient="records")

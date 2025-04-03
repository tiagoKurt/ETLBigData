import json
import os
import sys
import time

import pandas as pd
from connectMongo import ConnectMongo

from exchange import QueueExchange


class InserirMongoDB:
    def __init__(self, colecao="produtos", database="loja"):
        self.conn = ConnectMongo().connect()
        self.database = database
        self.colecao = colecao

    def inserirColecao(self):

        try:
            client = ConnectMongo()
            conexao = client.connect()
            db = conexao[self.database]
            print("Coleção nao existe no MONGODB", flush=True)
            csv = pd.read_csv("products.csv")
            lista_inserir = []
            json_data = csv.to_dict(orient="records")
            print(f"Produtos Carregados: {json_data}", flush=True)
            for item in json_data:
                objeto_tratado = {
                    "id": item["Product ID"],
                    "nome": item["Product Name"],
                    "categoriaProduto": item["Product Category"],
                    "descricaoProduto": item["Product Description"],
                    "precoProduto": item["Price"],
                    "quantidadeProduto": item["Stock Quantity"],
                    "periodoGarantia": item["Warranty Period"],
                    "dimensaoProduto": item["Product Dimensions"],
                    "dataProducao": item["Manufacturing Date"],
                    "dataExpiracao": item["Expiration Date"],
                    "tagsProdutos": item["Product Tags"],
                    "variacoesDeCorETamanho": item["Color/Size Variations"],
                    "ratingProduto": item["Product Ratings"],
                }

                lista_inserir.append(objeto_tratado)

            collection = db.create_collection(self.colecao)
            print(f"Produtos Inseridos: {lista_inserir}", flush=True)
            result = collection.insert_many(lista_inserir)
            print(f"Coleção Criada!: {result}", flush=True)
        except Exception as e:
            print(e)
        finally:
            exchange.sendMsg(
                {"colecao": self.colecao, "database": self.database, "status": True},
                rkey="etlmongodb-transformardados_service_tiago",
            )


def callback(ch, method, properties, body):
    print("SERVICE INSERIRMONGODB RUNNING", flush=True)
    dado = body.decode("utf-8")
    print(f" [x] Received {dado}", flush=True)
    dado = json.loads(dado)
    if not dado["status"]:
        inserir = InserirMongoDB()
        inserir.inserirColecao()


exchange = QueueExchange(
    host="rabbitmq",
    service="etlmongodb-inserirmongodb",
    rabbit_callback=callback,
    client="tiago",
)
exchange.start_consuming()

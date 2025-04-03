import json
import os

from connectMongo import ConnectMongo

from exchange import QueueExchange


class Verificador:
    def __init__(self, colecao, database):
        self.conn = ConnectMongo().connect()
        self.colecao = colecao
        self.database = database

    def verificarVariaveisDeExecucao(self):
        caminho = "/parquet/dados.parquet"
        if not os.path.isfile(caminho):
            database = self.conn[self.database]
            databases = self.conn.list_database_names()
            if self.database in databases:
                collections = database.list_collection_names()
                if self.colecao in collections:
                    exchange.sendMsg(
                        {
                            "colecao": self.colecao,
                            "database": self.database,
                            "status": True,
                        },
                        rkey="etlmongodb-transformardados_service_tiago",
                    )
                else:
                    print("COLECAO NÃO EXISTE: ", flush=True)
                    exchange.sendMsg(
                        {
                            "colecao": self.colecao,
                            "database": self.database,
                            "status": False,
                        },
                        rkey="etlmongodb-inserirmongodb_service_tiago",
                    )
            else:
                print("DATABASE NÃO EXISTE: ", flush=True)
                exchange.sendMsg(
                    {
                        "colecao": self.colecao,
                        "database": self.database,
                        "status": False,
                    },
                    rkey="etlmongodb-inserirmongodb_service_tiago",
                )


def callback(ch, method, properties, body):
    print("SERVICE VERIFICADOR RUNNING", flush=True)
    dado = body.decode("utf-8")
    print(f" [x] Received {dado}", flush=True)
    dado = json.loads(dado)
    print(dado, flush=True)
    if dado["status"] == False:
        verified = Verificador(dado["colecao"], dado["database"])
        verified.verificarVariaveisDeExecucao()


exchange = QueueExchange(
    host="rabbitmq",
    service="etlmongodb-verificador",
    rabbit_callback=callback,
    client="tiago",
)
exchange.start_consuming()

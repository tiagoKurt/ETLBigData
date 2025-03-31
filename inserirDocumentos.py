import pandas as pd

from connectMongo import ConnectMongo

# Send a ping to confirm a successful connection
try:
    csv = pd.read_csv("products.csv")
    lista_inserir = []
    json_data = csv.to_dict(orient="records")
    client = ConnectMongo()
    conexao = client.connect()
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

    db = conexao["loja"]
    collection = db.create_collection("produtos/teste/.env")
    result = collection.insert_many(lista_inserir)

except Exception as e:
    print(e)

import uvicorn
from fastapi import APIRouter, FastAPI

from pegarDadosETratarMongo import GetDataFromMongo

router = APIRouter()


@router.get("/todos")
def pegarProdutos():
    getData = GetDataFromMongo()
    return getData.get_data()


app = FastAPI()

# Adicionando o prefixo '/api/v1' para todas as rotas do router
app.include_router(router, prefix="/api/v1")


if __name__ == "__main__":
    uvicorn.run("apiTeste:app", host="127.0.0.1", port=8000, reload=True)

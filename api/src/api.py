import uvicorn
from fastapi import APIRouter, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from getDadosMongo import GetDadosMongo

router = APIRouter()


@router.get("/todos")
def pegarProdutos():
    getData = GetDadosMongo("produtos", "loja")
    return getData.getData()


app = FastAPI()

# Adicionando o prefixo '/api/v1' para todas as rotas do router
app.add_middleware(GZipMiddleware, minimum_size=1000)
app.include_router(router, prefix="/api/v1")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "https://dataflowx.tigasolutions.com.br",
        "http://107.155.87.251:3000/",
    ],  # Permite todas as origens (use uma lista específica para segurança)
    allow_credentials=True,
    allow_methods=["*"],  # Permite todos os métodos (GET, POST, PUT, DELETE, etc.)
    allow_headers=["*"],  # Permite todos os cabeçalhos
)
if __name__ == "__main__":
    uvicorn.run("api:app", host="0.0.0.0", port=8000, reload=True)

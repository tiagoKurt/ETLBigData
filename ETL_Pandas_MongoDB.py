import pandas as pd
import numpy as np
import pymongo
import os
from pymongo import MongoClient

# Função para criar diretório se não existir
def criar_diretorio(path):
    if not os.path.exists(path):
        os.makedirs(path)
        print(f"Diretório criado: {path}")

# Configurações de conexão com o MongoDB (mesma conexão do arquivo original)
client = MongoClient("mongodb://localhost:27017/")
db = client["powerliftingDB"]
collection = db["athletes"]

print("Conectado ao MongoDB com sucesso!")

# 1. EXTRAÇÃO: Ler dados do MongoDB
print("Extraindo dados do MongoDB...")
cursor = collection.find({})
df = pd.DataFrame(list(cursor))

print(f"Dados extraídos do MongoDB com sucesso!")
print(f"Total de registros: {len(df)}")
print("Colunas dos dados:")
print(df.columns.tolist())

# 2. TRANSFORMAÇÃO: Limpeza e formatação dos dados
print("\nIniciando transformação dos dados...")

# Remover registros duplicados
df_transformado = df.drop_duplicates()
print(f"Registros após remoção de duplicatas: {len(df_transformado)}")

# Remover espaços em branco dos campos de texto
string_columns = df_transformado.select_dtypes(include=['object']).columns
for column in string_columns:
    if column in df_transformado.columns:
        df_transformado[column] = df_transformado[column].astype(str).str.strip()

# Substituir valores nulos
df_transformado[string_columns] = df_transformado[string_columns].fillna("N/A")
numeric_columns = df_transformado.select_dtypes(include=['int64', 'float64']).columns
df_transformado[numeric_columns] = df_transformado[numeric_columns].fillna(0)

# Criar coluna para validação de dados
df_transformado['dados_validos'] = df_transformado['_id'].notnull()

print("Transformação concluída!")
print(f"Total de registros após transformação: {len(df_transformado)}")

# 3. CARGA: Salvar dados processados em arquivo
output_dir = "C:\\Users\\lezza\\Downloads\\archive\\dadosProcessados"
csv_output_path = "C:\\Users\\lezza\\Downloads\\archive\\dadosProcessados.csv"

# Criar diretório se não existir
criar_diretorio(output_dir)

# Salvar em CSV
df_transformado.to_csv(csv_output_path, index=False)
print(f"Dados processados salvos com sucesso em: {csv_output_path}")

# Salvar em formato parquet
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    
    parquet_path = os.path.join(output_dir, "dados.parquet")
    df_transformado.to_parquet(parquet_path, index=False)
    print(f"Dados processados salvos com sucesso em formato Parquet: {parquet_path}")
except ImportError:
    print("Biblioteca pyarrow não encontrada. Instalando...")
    import subprocess
    subprocess.run(["pip", "install", "pyarrow"], check=True)
    
    import pyarrow as pa
    import pyarrow.parquet as pq
    
    parquet_path = os.path.join(output_dir, "dados.parquet")
    df_transformado.to_parquet(parquet_path, index=False)
    print(f"Dados processados salvos com sucesso em formato Parquet: {parquet_path}")

print("\nPipeline ETL concluído com sucesso!") 
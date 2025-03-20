# Pipeline ETL para Dados de Powerlifting

Este projeto implementa um pipeline ETL (Extract, Transform, Load) para processar dados do dataset de Powerlifting armazenados no MongoDB.

## Estrutura do Projeto

- `InsercaoDadosMongoDB.py`: Script para inserção de dados CSV no MongoDB
- `ETL_PySpark_MongoDB.py`: Pipeline ETL usando PySpark (para ambientes com Hadoop configurado)
- `ETL_Pandas_MongoDB.py`: Pipeline ETL usando Pandas (solução alternativa sem dependência do Hadoop)

## Dataset

O projeto utiliza o dataset de Powerlifting disponível no Kaggle:
[Powerlifting Database](https://www.kaggle.com/datasets/dansbecker/powerlifting-database)

## Requisitos

- Python 3.6+
- MongoDB instalado e rodando localmente
- Pandas, PyMongo (para versão Pandas)
- PySpark, PyMongo (para versão PySpark - requer configuração adicional no Windows)
- PyArrow (para salvar arquivos Parquet)

## Instalação das Dependências

```bash
pip install pandas pymongo pyarrow
```

Para a versão PySpark (requer configuração adicional do Hadoop no Windows):
```bash
pip install pyspark pymongo findspark
```

## Como Executar

1. Certifique-se que o MongoDB está rodando na porta padrão (27017)
2. Execute o pipeline ETL:

```bash
# Versão Pandas (recomendada para Windows)
python ETL_Pandas_MongoDB.py

# Versão PySpark (para ambientes com Hadoop configurado)
python ETL_PySpark_MongoDB.py
```

## Descrição do Pipeline ETL

### 1. Extração (Extract)
- Conecta ao MongoDB na coleção "athletes" do banco "powerliftingDB"
- Extrai todos os registros em um DataFrame

### 2. Transformação (Transform)
- Adiciona uma coluna de validação de dados
- Remove registros duplicados
- Limpa espaços em branco em campos de texto
- Substitui valores nulos por padrões adequados

### 3. Carga (Load)
- Salva os dados processados em formato Parquet e CSV
- Os arquivos são armazenados no diretório configurado

## Observações

- A versão Pandas é recomendada para Windows, pois não depende da configuração do Hadoop
- A versão PySpark é mais adequada para processamento de grandes volumes de dados, mas requer configuração adicional no Windows
- As transformações podem ser personalizadas conforme necessário 
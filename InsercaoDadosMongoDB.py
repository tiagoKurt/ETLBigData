import pandas as pd
from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")

db = client["powerliftingDB"]
collection = db["athletes"]

file_path = "C:\\Users\\lezza\\Downloads\\archive\\meets.csv" 

df = pd.read_csv(file_path)

records = df.to_dict(orient="records")

collection.insert_many(records)

print("Dados inseridos com sucesso no MongoDB!")

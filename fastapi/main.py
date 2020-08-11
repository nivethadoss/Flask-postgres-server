from fastapi import FastAPI
import psycopg2
import databases, sqlalchemy
from typing import List
import uvicorn
from pydantic import BaseModel, Field

DATABASE_URL = "postgresql://postgres:postgres@postgres:5432"

database = databases.Database(DATABASE_URL)
metadata = sqlalchemy.MetaData()
engine = sqlalchemy.create_engine(DATABASE_URL)
metadata.create_all(engine)


con = psycopg2.connect(host="localhost", port = "5432", database="postgres", user="postgres", password="postgres")
cursor = con.cursor()
con.commit()



## Models
class Station_id_list(BaseModel):
    station_id : str
    hardware : enumerate

app = FastAPI()

@app.get("/station_id", response_model=List[Station_id_list])
async def display_all_statioId():
    query = "Select * from stations;"
    return await database.fetch_all(query)



if __name__== "__main__":
    uvicorn.run(app, host = "0.0.0.0", port=8000)
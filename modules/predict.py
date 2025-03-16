import json
import dill
import pandas as pd
from fastapi import FastAPI
from pydantic import BaseModel
from typing import Optional
import uvicorn
import glob
from datetime import datetime

model_path = "airflow_hw/data/models/cars_pipe_202503152040.pkl"
with open(model_path, "rb") as file:
    model_data = dill.load(file)

model = model_data


def predict():
    forms = glob.glob("airflow_hw/data/test/"+"*.json")
    new_data = {}
    y_preds = []
    id_list = []
    
    for formfile in forms:
        
        with open(formfile, "r") as file:
            form = json.load(file)

        df = pd.DataFrame.from_dict([form])
        
        try:
            y_pred = model.predict(df)[0]
            id_list.append(df['id'].values[0])
        except Exception as e:
            print("ebany_rot_etogo_kazino", e)
            
        y_preds.append(y_pred)
        
    new_data = {"car_id": id_list,"name ": y_preds}
    
    df_pred = pd.DataFrame(new_data)
    df_name = f'airflow_hw/data/predictions/pred_{datetime.now().strftime("%Y%m%d%H%M")}.csv'
    df_pred.to_csv(df_name, index=False)

    
if __name__ == '__main__':
    predict()

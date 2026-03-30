import sys
import os
import pandas as pd
import io
import aiohttp
from fastapi import FastAPI, UploadFile, File, HTTPException
from sqlalchemy import create_engine
from pydantic import BaseModel

sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from scripts.ai_agent import AIAgent
from scripts.data_cleaning import DataCleaning

app = FastAPI()

ai_agent = AIAgent()
cleaner = DataCleaning()

# =========================================================
# NORMALIZATION
# =========================================================

def normalize_dataframe(df):
    df = df.astype(str)
    df = df.apply(lambda x: x.str.strip())
    df.replace(["", "nan", "None"], None, inplace=True)
    return df


# =========================================================
# SAFE AI OUTPUT
# =========================================================

def safe_convert_to_dataframe(data, fallback_df):
    import json
    from io import StringIO

    if isinstance(data, pd.DataFrame):
        return data

    if isinstance(data, str):
        try:
            return pd.DataFrame(json.loads(data))
        except:
            pass

        try:
            return pd.read_csv(StringIO(data), on_bad_lines="skip", engine="python")
        except:
            pass

    return fallback_df


# =========================================================
# 🔥 FINAL JSON SAFE FUNCTION (MOST IMPORTANT)
# =========================================================

def dataframe_to_safe_json(df):
    return df.fillna("").astype(object).to_dict(orient="records")


# =========================================================
# FILE CLEANING
# =========================================================

@app.post("/clean-data")
async def clean_data(file: UploadFile = File(...)):
    try:
        contents = await file.read()
        ext = file.filename.split(".")[-1].lower()

        if ext == "csv":
            try:
                df = pd.read_csv(io.StringIO(contents.decode("utf-8")), on_bad_lines="skip", engine="python")
            except:
                df = pd.read_csv(io.StringIO(contents.decode("latin-1")), on_bad_lines="skip", engine="python")

        elif ext == "xlsx":
            df = pd.read_excel(io.BytesIO(contents), engine="openpyxl", dtype=str)

        else:
            raise HTTPException(status_code=400, detail="Use CSV or Excel")

        df = normalize_dataframe(df)
        df.dropna(how="all", inplace=True)
        df.columns = df.columns.astype(str)

        df_cleaned = cleaner.clean_data(df)

        try:
            df_ai_cleaned = ai_agent.process_data(df_cleaned)
            df_ai_cleaned = safe_convert_to_dataframe(df_ai_cleaned, df_cleaned)
        except Exception as e:
            print("AI ERROR:", e)
            df_ai_cleaned = df_cleaned

        if not isinstance(df_ai_cleaned, pd.DataFrame):
            df_ai_cleaned = df_cleaned

        return {
            "cleaned_data": dataframe_to_safe_json(df_ai_cleaned)  # ✅ FIX
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =========================================================
# DATABASE CLEANING
# =========================================================

class DBQuery(BaseModel):
    db_url: str
    query: str


@app.post("/clean-db")
async def clean_db(query: DBQuery):
    try:
        engine = create_engine(query.db_url)
        df = pd.read_sql(query.query, engine)

        df = normalize_dataframe(df)
        df.dropna(how="all", inplace=True)
        df.columns = df.columns.astype(str)

        df_cleaned = cleaner.clean_data(df)

        try:
            df_ai_cleaned = ai_agent.process_data(df_cleaned)
            df_ai_cleaned = safe_convert_to_dataframe(df_ai_cleaned, df_cleaned)
        except Exception as e:
            print("AI ERROR:", e)
            df_ai_cleaned = df_cleaned

        if not isinstance(df_ai_cleaned, pd.DataFrame):
            df_ai_cleaned = df_cleaned

        return {
            "cleaned_data": dataframe_to_safe_json(df_ai_cleaned)  # ✅ FIX
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =========================================================
# API CLEANING
# =========================================================

class APIRequest(BaseModel):
    api_url: str


@app.post("/clean-api")
async def clean_api(req: APIRequest):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(req.api_url) as response:

                if response.status != 200:
                    raise HTTPException(status_code=400, detail="API failed")

                data = await response.json()
                df = pd.DataFrame(data)

        df = normalize_dataframe(df)
        df.dropna(how="all", inplace=True)
        df.columns = df.columns.astype(str)

        df_cleaned = cleaner.clean_data(df)

        try:
            df_ai_cleaned = ai_agent.process_data(df_cleaned)
            df_ai_cleaned = safe_convert_to_dataframe(df_ai_cleaned, df_cleaned)
        except Exception as e:
            print("AI ERROR:", e)
            df_ai_cleaned = df_cleaned

        if not isinstance(df_ai_cleaned, pd.DataFrame):
            df_ai_cleaned = df_cleaned

        return {
            "cleaned_data": dataframe_to_safe_json(df_ai_cleaned)  # ✅ FIX
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =========================================================
# RUN SERVER
# =========================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("scripts.backend:app", host="127.0.0.1", port=8000, reload=True)

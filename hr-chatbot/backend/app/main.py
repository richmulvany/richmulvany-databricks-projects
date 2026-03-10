from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from app.agent import ask_agent, ask_agent_debug, get_database
import logging


app = FastAPI(title="Databricks SQL Agent")
logger = logging.getLogger("databricks_agent")

@app.on_event("startup")
def warmup_databricks():
    try:
        db = get_database()
        with db.engine.connect() as conn:
            conn.execute("SELECT 1")
        logger.info("Databricks warehouse warmup successful")
    except Exception as e:
        logger.warning("Databricks warehouse warmup failed: %s", e)

@app.get("/ask")
def ask(question: str):
    if not question.strip():
        raise HTTPException(status_code=400, detail="Question cannot be empty")
    try:
        response = ask_agent(question)
        return {"question": question, "response": response}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/ask_debug")
def ask_debug(question: str):
    if not question.strip():
        raise HTTPException(status_code=400, detail="Question cannot be empty")
    try:
        return ask_agent_debug(question)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

app.mount("/", StaticFiles(directory="frontend_build", html=True), name="frontend")
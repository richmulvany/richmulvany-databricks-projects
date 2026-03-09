from fastapi import FastAPI, HTTPException
from app.agent import ask_agent

app = FastAPI()

@app.get("/ask")
def ask(question: str):
    try:
        response = ask_agent(question)
        return {"response": response}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
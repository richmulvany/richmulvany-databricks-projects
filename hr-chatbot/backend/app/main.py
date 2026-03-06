from fastapi import FastAPI
from app.agent import ask_agent

app = FastAPI()

@app.get("/ask")
def ask(question: str):
    return {"response": ask_agent(question)}
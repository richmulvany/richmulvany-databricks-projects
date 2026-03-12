import asyncio
import uuid
import logging
from concurrent.futures import ThreadPoolExecutor

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from app.agent import ask_agent

logging.basicConfig(level=logging.INFO)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class QuestionRequest(BaseModel):
    question: str
    session_id: str | None = None


@app.post("/ask_stream")
async def ask_stream(request: QuestionRequest):

    if not request.question.strip():
        raise HTTPException(status_code=400, detail="Question cannot be empty")

    session_id = request.session_id or str(uuid.uuid4())

    loop = asyncio.get_event_loop()
    executor = ThreadPoolExecutor(max_workers=1)

    def run_agent():
        return ask_agent(request.question, session_id)

    future = loop.run_in_executor(executor, run_agent)

    async def stream():

        while not future.done():
            await asyncio.sleep(0.05)

        result = await future

        for step in result["trace"]:
            yield f"trace|{step}\n"
            await asyncio.sleep(0.05)

        for token in result["answer"].split(" "):
            yield f"bot|{token} \n"
            await asyncio.sleep(0.02)

    return StreamingResponse(stream(), media_type="text/plain")
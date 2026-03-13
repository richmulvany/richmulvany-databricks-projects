import os
import json
import logging

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, FileResponse
from fastapi.staticfiles import StaticFiles

from app.agent import app_graph

logging.basicConfig(level=logging.INFO)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

frontend_dir = os.path.join(os.getcwd(), "frontend_build")

if os.path.exists(frontend_dir):

    app.mount(
        "/static",
        StaticFiles(directory=os.path.join(frontend_dir, "static")),
        name="static"
    )

    @app.get("/")
    async def serve_frontend():
        return FileResponse(os.path.join(frontend_dir, "index.html"))


@app.get("/ask_stream")
async def ask_stream(question: str):

    def event_generator():

        state = {
            "question": question,
            "tables": None,
            "schema": None,
            "sql_query": None,
            "sql_error": None,
            "results": None,
            "answer": None,
            "trace": [],
            "retry_count": 0
        }

        last_trace_len = 0

        for step in app_graph.stream(state):

            # Each step is {"node_name": state_update}
            for node, update in step.items():

                # STREAM TRACE
                if "trace" in update:

                    trace = update["trace"]

                    if len(trace) > last_trace_len:

                        new_entries = trace[last_trace_len:]

                        for entry in new_entries:
                            yield f"data: {json.dumps({'trace': entry})}\n\n"

                        last_trace_len = len(trace)

                # STREAM FINAL ANSWER
                if "answer" in update:

                    yield f"data: {json.dumps({'answer': update['answer']})}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream"
    )
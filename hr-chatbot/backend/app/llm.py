from langchain_openai import ChatOpenAI
import os

def get_llm():
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("OPENAI_API_KEY must be set")
    return ChatOpenAI(
        model="gpt-4o-mini",
        api_key=api_key,
        temperature=0  # deterministic SQL responses
    )
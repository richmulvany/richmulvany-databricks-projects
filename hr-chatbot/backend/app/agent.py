from langchain.agents import create_sql_agent
from app.db import get_database
from app.llm import get_llm

def ask_agent(question):
    db = get_database()
    llm = get_llm()

    agent = create_sql_agent(llm, db=db)
    return agent.run(question)
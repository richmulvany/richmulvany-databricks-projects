from langchain.agents import create_agent
from langchain.agents.agent_toolkits import SQLDatabaseToolkit
from app.db import get_database
from app.llm import get_llm

def ask_agent(question):
    db = get_database()
    llm = get_llm()

    toolkit = SQLDatabaseToolkit(db=db, llm=llm)
    agent = create_agent(llm, toolkit, verbose=True)
    return agent.run(question)
from langchain.agents import create_agent
from langchain.agents.agent_toolkits import SQLDatabaseToolkit
from langchain.agents.agent_types import AgentType
from app.db import get_database
from app.llm import get_llm

def ask_agent(question):
    db = get_database()
    llm = get_llm()

    toolkit = SQLDatabaseToolkit(db=db, llm=llm)
    agent = create_agent(
        llm=llm,
        toolkit=toolkit,
        agent_type=AgentType.ZERO_SHOT_REACT_DESCRIPTION,
        verbose=True
    )
    return agent.run(question)
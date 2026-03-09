from langchain_community.agent_toolkits import create_sql_agent
from langchain_community.agent_toolkits.sql.toolkit import SQLDatabaseToolkit
from app.db import get_database
from app.llm import get_llm

def ask_agent(question):
    db = get_database()
    llm = get_llm()

    toolkit = SQLDatabaseToolkit(db=db, llm=llm)

    agent = create_sql_agent(
        llm=llm,
        toolkit=toolkit,
        verbose=True
    )

    return agent.run(question)
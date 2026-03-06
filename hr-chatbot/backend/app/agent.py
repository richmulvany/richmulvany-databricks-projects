from langchain_experimental.sql import SQLDatabaseChain
from langchain_experimental.sql import SQLDatabase
from app.db import get_database
from app.llm import get_llm

def ask_agent(question):
    db = get_database()  # this should return a SQLAlchemy engine or connection
    llm = get_llm()

    # Wrap your database
    db_wrapper = SQLDatabase(engine=db)  # or SQLDatabase.from_uri("sqlite:///my.db")

    # Create the chain/agent
    db_chain = SQLDatabaseChain(llm=llm, database=db_wrapper, verbose=True)

    return db_chain.run(question)
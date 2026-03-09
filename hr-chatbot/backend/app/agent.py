import logging
import time
from sqlalchemy.exc import OperationalError
from langchain_community.agent_toolkits import create_sql_agent
from langchain_community.agent_toolkits.sql.toolkit import SQLDatabaseToolkit
from app.db import get_database
from app.llm import get_llm

# Logging to file and stdout
logger = logging.getLogger("databricks_agent")
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler("agent.log")
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

MAX_RETRIES = 2
RETRY_DELAY = 5  # seconds

def run_with_retry(agent, question):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return agent.run(input=question, return_intermediate_steps=False)
        except OperationalError:
            logger.warning("Databricks warehouse not ready, retrying... (%d/%d)", attempt, MAX_RETRIES)
            time.sleep(RETRY_DELAY)
    raise RuntimeError("Databricks warehouse did not respond after retries")

def ask_agent(question: str) -> str:
    db = get_database()
    llm = get_llm()
    toolkit = SQLDatabaseToolkit(db=db, llm=llm)
    agent = create_sql_agent(llm=llm, toolkit=toolkit, verbose=True)

    logger.info("Question received: %s", question)
    response = run_with_retry(agent, question)
    logger.info("Response: %s", response)
    return response

def ask_agent_debug(question: str) -> dict:
    """
    Returns both the generated SQL and final answer.
    """
    db = get_database()
    llm = get_llm()
    toolkit = SQLDatabaseToolkit(db=db, llm=llm)
    agent = create_sql_agent(llm=llm, toolkit=toolkit, verbose=True)

    logger.info("Debug question received: %s", question)
    # return_intermediate_steps=True gives steps including generated SQL
    result = run_with_retry(agent, question)
    return {
        "question": question,
        "answer": result
    }
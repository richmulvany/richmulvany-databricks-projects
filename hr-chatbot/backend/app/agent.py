import logging
from typing import TypedDict, Optional, List, Dict, Any

from langchain_core.prompts import ChatPromptTemplate
from langchain_community.utilities import SQLDatabase
from langgraph.graph import StateGraph, END

from app.llm import get_llm
from app.db import get_engine

logger = logging.getLogger("databricks_agent")


class AgentState(TypedDict):
    question: str
    sql_query: Optional[str]
    sql_error: Optional[str]
    results: Optional[List[Dict[str, Any]]]
    answer: Optional[str]
    trace: List[str]
    retry_count: int


llm = get_llm()

engine = get_engine()
db = SQLDatabase(engine)


def get_schema():
    return db.get_table_info()


sql_prompt = ChatPromptTemplate.from_template(
"""
You are a data analyst.

Generate a SQL query to answer the user's question.

Database schema:
{schema}

Rules:
- Only return SQL
- No explanation

Question:
{question}
"""
)

fix_prompt = ChatPromptTemplate.from_template(
"""
The following SQL query failed.

Schema:
{schema}

Query:
{sql}

Error:
{error}

Rewrite the SQL to fix the issue.

Return ONLY SQL.
"""
)

answer_prompt = ChatPromptTemplate.from_template(
"""
User question:
{question}

SQL executed:
{sql}

Query results:
{results}

Provide a clear answer.
"""
)


def generate_sql(state: AgentState):

    schema = get_schema()

    response = llm.invoke(
        sql_prompt.format(
            schema=schema,
            question=state["question"]
        )
    )

    sql = response.content.strip()

    state["trace"].append(f"Generated SQL:\n{sql}")

    return {"sql_query": sql}


def run_sql(state: AgentState):

    sql = state["sql_query"]

    try:

        with engine.connect() as conn:

            result = conn.exec_driver_sql(
                sql,
                execution_options={"timeout": 30}
            )

            rows = result.fetchall()
            columns = result.keys()

        preview = [
            dict(zip(columns, row))
            for row in rows[:20]
        ]

        state["trace"].append(f"SQL Result Preview:\n{preview}")

        return {
            "results": preview,
            "sql_error": None
        }

    except Exception as e:

        logger.info(f"SQL error: {e}")

        return {"sql_error": str(e)}


def fix_sql(state: AgentState):

    retry_count = state["retry_count"] + 1

    schema = get_schema()

    response = llm.invoke(
        fix_prompt.format(
            schema=schema,
            sql=state["sql_query"],
            error=state["sql_error"]
        )
    )

    sql = response.content.strip()

    state["trace"].append(f"Retry {retry_count}: Fixed SQL\n{sql}")

    return {
        "sql_query": sql,
        "sql_error": None,
        "retry_count": retry_count
    }


def explain(state: AgentState):

    response = llm.invoke(
        answer_prompt.format(
            question=state["question"],
            sql=state["sql_query"],
            results=state["results"]
        )
    )

    return {"answer": response.content}


def fail(state: AgentState):

    return {
        "answer": "I couldn't generate a valid SQL query after several attempts. Please try rephrasing your question."
    }


def route_sql(state: AgentState):

    if state.get("sql_error"):

        if state["retry_count"] >= 3:
            return "fail"

        return "fix_sql"

    return "explain"


graph = StateGraph(AgentState)

graph.add_node("generate_sql", generate_sql)
graph.add_node("run_sql", run_sql)
graph.add_node("fix_sql", fix_sql)
graph.add_node("explain", explain)
graph.add_node("fail", fail)

graph.set_entry_point("generate_sql")

graph.add_edge("generate_sql", "run_sql")

graph.add_conditional_edges(
    "run_sql",
    route_sql,
    {
        "fix_sql": "fix_sql",
        "explain": "explain",
        "fail": "fail"
    }
)

graph.add_edge("fix_sql", "run_sql")
graph.add_edge("explain", END)
graph.add_edge("fail", END)

app_graph = graph.compile()


def ask_agent(question: str, session_id: str):

    state = {
        "question": question,
        "sql_query": None,
        "sql_error": None,
        "results": None,
        "answer": None,
        "trace": [],
        "retry_count": 0
    }

    result = app_graph.invoke(state)

    return {
        "answer": result["answer"],
        "trace": result["trace"]
    }
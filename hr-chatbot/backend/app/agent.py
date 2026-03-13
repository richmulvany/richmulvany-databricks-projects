import logging
import re
from typing import TypedDict, Optional, List, Dict, Any
from functools import lru_cache

from langchain_core.prompts import ChatPromptTemplate
from langchain_community.utilities import SQLDatabase
from langgraph.graph import StateGraph, END

from app.db import get_engine
from app.llm import get_llm

logger = logging.getLogger("sql_agent")
logging.getLogger("databricks.sql").setLevel(logging.ERROR)
logging.getLogger("urllib3").setLevel(logging.ERROR)


class AgentState(TypedDict):
    question: str
    tables: Optional[List[str]]
    schema: Optional[Dict[str, Any]]
    sql_query: Optional[str]
    sql_error: Optional[str]
    results: Optional[List[Dict[str, Any]]]
    answer: Optional[str]
    trace: List[str]
    retry_count: int


# ------------------------
# METADATA CACHE
# ------------------------

@lru_cache(maxsize=1)
def list_tables():
    engine = get_engine()
    db = SQLDatabase(engine)
    return db.get_usable_table_names()


def get_table_schema(tables: List[str]):
    engine = get_engine()

    if not tables:
        raise ValueError("No tables specified")

    db = SQLDatabase(engine, include_tables=tables)
    table_info = db.get_table_info()

    fq_table_info = {}

    for t in tables:
        fq_name = f"03_gold.hr.{t}"
        fq_table_info[fq_name] = table_info

    return fq_table_info


# ------------------------
# SQL SANITISATION
# ------------------------

def clean_sql(sql: str) -> str:
    sql = sql.strip()
    sql = re.sub(r"```sql", "", sql, flags=re.IGNORECASE)
    sql = sql.replace("```", "")
    return sql.strip()


def enforce_sql_safety(sql: str) -> str:
    forbidden = ["DROP", "DELETE", "UPDATE", "INSERT", "ALTER", "TRUNCATE"]

    upper = sql.upper()

    for keyword in forbidden:
        if keyword in upper:
            raise ValueError("Dangerous SQL operation blocked")

    if not upper.startswith("SELECT"):
        raise ValueError("Only SELECT queries allowed")

    return sql


def enforce_row_limit(sql: str, limit=100) -> str:
    if "LIMIT" not in sql.upper():
        sql = sql.rstrip(";") + f" LIMIT {limit}"
    return sql


def qualify_sql_tables(sql: str, schema_dict: Dict[str, Any]) -> str:

    for fq_table in schema_dict.keys():
        table_name = fq_table.split(".")[-1]
        pattern = rf"(?<!\.)\b{table_name}\b"
        sql = re.sub(pattern, fq_table, sql)

    return sql


# ------------------------
# PROMPTS
# ------------------------

table_select_prompt = ChatPromptTemplate.from_template(
"""
Choose which tables are relevant.

Tables:
{tables}

Question:
{question}

Return comma separated table names.
"""
)

sql_prompt = ChatPromptTemplate.from_template(
"""
Generate SQL for this question.

Schema:
{schema}

Rules:
- Only SQL
- No explanation

Question:
{question}
"""
)

fix_prompt = ChatPromptTemplate.from_template(
"""
This SQL failed.

Schema:
{schema}

Query:
{sql}

Error:
{error}

Rewrite the SQL.

Return only SQL.
"""
)

answer_prompt = ChatPromptTemplate.from_template(
"""
User question:
{question}

SQL executed:
{sql}

Results:
{results}

Provide a concise answer.
"""
)


# ------------------------
# GRAPH NODES
# ------------------------

def select_tables(state: AgentState):

    llm = get_llm()
    tables = list_tables()

    response = llm.invoke(
        table_select_prompt.format(
            tables=", ".join(tables),
            question=state["question"]
        )
    )

    chosen = [t.strip() for t in response.content.split(",") if t.strip() in tables]

    state["trace"].append(f"###### Selected tables:\n{chosen}")

    return {"tables": chosen, "trace": state["trace"]}


def load_schema(state: AgentState):

    schema = get_table_schema(state["tables"])

    state["trace"].append("###### Loaded schema")

    return {"schema": schema, "trace": state["trace"]}


def generate_sql(state: AgentState):

    llm = get_llm()

    response = llm.invoke(
        sql_prompt.format(
            schema=state["schema"],
            question=state["question"]
        )
    )

    sql = clean_sql(response.content)
    sql = qualify_sql_tables(sql, state["schema"])
    sql = enforce_sql_safety(sql)
    sql = enforce_row_limit(sql)

    state["trace"].append(f"###### Generated SQL:\n{sql}")

    return {"sql_query": sql, "trace": state["trace"]}


def run_sql(state: AgentState):

    engine = get_engine()

    try:

        state["trace"].append("###### Running SQL query")

        with engine.connect() as conn:

            result = conn.exec_driver_sql(
                state["sql_query"],
                execution_options={"timeout": 30}
            )

            rows = result.fetchmany(20)
            columns = result.keys()

        preview = [dict(zip(columns, row)) for row in rows]

        state["trace"].append(f"###### Result preview:\n{preview}")

        return {"results": preview, "sql_error": None, "trace": state["trace"]}

    except Exception as e:

        logger.info(e)

        return {"sql_error": str(e), "trace": state["trace"]}


def fix_sql(state: AgentState):

    llm = get_llm()

    retry_count = state["retry_count"] + 1

    response = llm.invoke(
        fix_prompt.format(
            schema=state["schema"],
            sql=state["sql_query"],
            error=state["sql_error"]
        )
    )

    sql = clean_sql(response.content)
    sql = qualify_sql_tables(sql, state["schema"])
    sql = enforce_sql_safety(sql)
    sql = enforce_row_limit(sql)

    state["trace"].append(f"######  Retry {retry_count}: {sql}")

    return {
        "sql_query": sql,
        "retry_count": retry_count,
        "trace": state["trace"]
    }


def explain(state: AgentState):

    llm = get_llm()

    response = llm.invoke(
        answer_prompt.format(
            question=state["question"],
            sql=state["sql_query"],
            results=state["results"]
        )
    )

    state["trace"].append("######  Generated final answer")

    return {"answer": response.content, "trace": state["trace"]}


def fail(state: AgentState):

    return {"answer": "I couldn't generate a valid SQL query."}


# ------------------------
# ROUTING
# ------------------------

def route_sql(state: AgentState):

    if state.get("sql_error"):

        if state["retry_count"] >= 3:
            return "fail"

        return "fix_sql"

    return "explain"


# ------------------------
# GRAPH
# ------------------------

graph = StateGraph(AgentState)

graph.add_node("select_tables", select_tables)
graph.add_node("load_schema", load_schema)
graph.add_node("generate_sql", generate_sql)
graph.add_node("run_sql", run_sql)
graph.add_node("fix_sql", fix_sql)
graph.add_node("explain", explain)
graph.add_node("fail", fail)

graph.set_entry_point("select_tables")

graph.add_edge("select_tables", "load_schema")
graph.add_edge("load_schema", "generate_sql")
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
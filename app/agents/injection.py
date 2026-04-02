"""
Injection Agent
---------------
Runs immediately after the Inspector Agent finishes (before the PR is merged).
Pushes accepted staging data into the newly created DEV_ tables so that
analytics and demos are available while the Production PR is pending.

This agent executes the injection dbt script the Inspector already wrote.
"""

from langchain_openai import ChatOpenAI
from langchain.agents import AgentExecutor, create_tool_calling_agent
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_core.tools import tool

from app.config import get_settings
from app.logging_config import get_logger
from app.services.dbt_runner import run_dbt

settings = get_settings()
log = get_logger("injection")


@tool
def run_injection_script(model_name: str) -> str:
    """Execute a specific dbt model to inject staging data into DEV_ tables.

    Args:
        model_name: the dbt model name, e.g. 'inject_a1b2c3d4'
    """
    log.info("Running dbt injection model: %s", model_name)
    returncode, output = run_dbt(["run", "--select", model_name])
    status = "SUCCESS" if returncode == 0 else "FAILED"
    log.info("Injection model %s → %s", model_name, status)
    return f"[{status}]\n{output}"


INJECTION_PROMPT = ChatPromptTemplate.from_messages([
    ("system", """\
You are the Injection Agent. The Inspector Agent has already written and committed
a dbt injection script that loads data from staging_raw into DEV_ tables.

Your job:
1. Run the injection dbt model using run_injection_script with the model name provided.
2. If it fails, diagnose the output and retry with a corrected invocation.
3. Report the final status clearly.

Injection model: {injection_model}
Cycle ID: {cycle_id}
"""),
    ("human", "Execute the injection for cycle {cycle_id}."),
    MessagesPlaceholder("agent_scratchpad"),
])


def run_injection(cycle_id: str, injection_model: str) -> str:
    """Entrypoint: run the Injection Agent and return final status."""
    log.info("Starting injection for cycle %s, model %s", cycle_id, injection_model)

    llm = ChatOpenAI(
        model=settings.xai_model,
        api_key=settings.xai_api_key,
        base_url=settings.xai_base_url,
        temperature=0,
    )

    tools = [run_injection_script]

    agent = create_tool_calling_agent(llm, tools, INJECTION_PROMPT)
    executor = AgentExecutor(
        agent=agent,
        tools=tools,
        verbose=False,
        max_iterations=10,
        handle_parsing_errors=True,
    )

    result = executor.invoke({"cycle_id": cycle_id, "injection_model": injection_model})
    output = result.get("output", "")
    log.info("Injection finished for cycle %s", cycle_id)
    return output

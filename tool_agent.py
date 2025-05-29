from llama_index.core.agent.workflow import FunctionAgent
from llama_index.core.tools import FunctionTool
from llama_index.core.agent.workflow import ToolCallResult, AgentStream

class ToolAgent:
    def __init__(self, tool, tool_name, tool_description, agent_prompt):
        self.tool = tool
        self.name = tool_name
        self.description = tool_description
        self.agent_prompt = agent_prompt

    async def tool_agent(self, query):
        function_tool = FunctionTool.from_defaults(
            self.tool,
            name=self.name,
            description=self.description
        )

        agent = FunctionAgent(
            tools=[function_tool],
            system_prompt=self.agent_prompt,
        )

        handler = agent.run(query)

        async for ev in handler.stream_events():
            if isinstance(ev, ToolCallResult):
                print(
                    f"Call {ev.tool_name} with args {ev.tool_kwargs}\nReturned: {ev.tool_output}"
                )
            # elif isinstance(ev, AgentStream):
            #     print(ev.delta, end="", flush=True)

        response = await handler
        
        print(f"Resposta: ${response}")
        return response
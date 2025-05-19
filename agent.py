import asyncio
from dotenv import load_dotenv

from query_engines import QueryEngine
from tools import generate_xlsx, load_dataframes
load_dotenv()

from llama_index.core.agent.workflow import FunctionAgent
from llama_index.core.tools import QueryEngineTool
from llama_index.core.tools import FunctionTool
from llama_index.core.agent.workflow import ToolCallResult, AgentStream


from llama_index.llms.anthropic import Anthropic
from llama_index.llms.openai import OpenAI

from llama_index.core import Settings


class Agent:
    def __init__(self):
        llm = Anthropic(model="claude-3-5-haiku-latest", temperature=0.5, timeout=None, max_retries=2)
        # llm = OpenAI(model="o3-mini", temperature=0.5, max_retries=2)
        
        Settings.llm = llm
        
        self.tools = None
        
        self._set_tools()
        self._init_agent()
        
    def _set_tools(self):
        colab = QueryEngine("data/Dados Colaboradores.xlsx")
        
        beneficio_unimef = QueryEngine("data/Beneficios/Beneficio 1 - Unimed.xlsx")
        
        colab_qe = QueryEngineTool.from_defaults(
            colab.get_QE(),
            name="QueryEngineColaboradores",
            description="Query engine utilizada para analisar os dados dos colaboradores. Deve ser passado uma query como parametro, que será convertida em um codigo pandas do Python"
        )
        
        unimed_qe = QueryEngineTool.from_defaults(
            beneficio_unimef.get_QE(),
            name="QueryEngineBeneficioUnimed",
            description="Query engine que pode ser utilizada para analisar dados do beneficio da unimed dos colaboradores. Deve ser passado uma query como parametro, que será convertida em um codigo pandas do Python."
        )
    
        
        self.tools = [colab_qe, unimed_qe]        
        
        
    def _init_agent(self):
        self.agent = FunctionAgent(
            tools=self.tools,
            system_prompt=(
                """
                    Você é um agente de IA autônomo que pode utilizar ferramentas para análisar planilhas, ler, padronizar e juntar dados de outras planilhas, e gerar outras planilhas.
                    As ferramentas utilizadas te possibilitam ler e manipular dados de diferentes fontes utilizando comandos da bilbioteca pandas.
                    Você possui dados de colaboradores, ferramentas utilizadas pelos colaboradores e benefícios e custos associados a cada colaborador.
                    Seu trabalho final gerar um .xlsx contendo o ID do colaborador, nome do colaborador, centro de custo, custo por cada ferramenta, custo por cada benefício e custo total.
                """
            )
        )
        
        
                
    async def run(self, query):
        
        handler = self.agent.run(query)

        async for ev in handler.stream_events():
            if isinstance(ev, ToolCallResult):
                print(
                    f"Call {ev.tool_name} with args {ev.tool_kwargs}\nReturned: {ev.tool_output}"
                )
            elif isinstance(ev, AgentStream):
                print(ev.delta, end="", flush=True)

        response = await handler
        
        print(f"Resposta: ${response}")
        return response
    
if __name__ == "__main__":
    agent = Agent()
    
    while True:
        print("Digite 'sair' para encerrar o programa.")
        question = input("Pergunta: ")
        if question.lower() == "sair":
            break
        else:
            response = asyncio.run(agent.run(question))
            print(f"Resposta: {response}")
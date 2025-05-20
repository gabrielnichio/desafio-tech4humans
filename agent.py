import asyncio
from dotenv import load_dotenv

import pandas as pd

from exec_func import PandasExecutor
from query_engines import QueryEngine
# from tools import generate_xlsx, load_dataframes
from exec_func import PandasExecutor
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
        colab = pd.read_excel("data/Dados Colaboradores.xlsx")
        google = pd.read_excel("data/Ferramentas/Ferramenta 2 - Google workspace.xlsx") 

        pandas_executor = PandasExecutor(colab, google)
    

        # colab = QueryEngine("data/Dados Colaboradores.xlsx")
        
        # beneficio_unimef = QueryEngine("data/Beneficios/Beneficio 1 - Unimed.xlsx")
        
        # colab_qe = QueryEngineTool.from_defaults(
        #     colab.get_QE(),
        #     name="QueryEngineColaboradores",
        #     description="Query engine utilizada para analisar os dados dos colaboradores. Deve ser passado uma query como parametro, que será convertida em um codigo pandas do Python"
        # )
        
        # unimed_qe = QueryEngineTool.from_defaults(
        #     beneficio_unimef.get_QE(),
        #     name="QueryEngineBeneficioUnimed",
        #     description="Query engine que pode ser utilizada para analisar dados do beneficio da unimed dos colaboradores. Deve ser passado uma query como parametro, que será convertida em um codigo pandas do Python."
        # )

        exec_code = FunctionTool.from_defaults(
            pandas_executor.execute,
            name="ExecutaCodigo",
            description="Funcao capaz de executar uma linha de codigo pandas em dataframes. A funcao recebe o parametro 'codigo', uma string contendo uma linha de codigo pandas. Apenas a biblioteca pandas(pd) esta disponivel no ambiente."
        )

        export_xlsx = FunctionTool.from_defaults(
            pandas_executor.export_df,
            name="ExportaDataframe",
            description="Funcao capaz de exportar o dataframe final."
        )
    
        
        self.tools = [exec_code, export_xlsx]        
        
        
    def _init_agent(self):
        self.agent = FunctionAgent(
            tools=self.tools,
            system_prompt=(
                """
                    Você é um agente especializado em análise de dados. Você tem a capacidade de manipular alguns dataframes através de funções. Você consegue manipular um df_final
                    que está disponível no ambiente de execução. Você possui o dataframe de colaboradores "colab" e o dataframe "google" que você também pode utilizar.

                    Através das suas ferramentas você pode manipular os dados dos dataframes.
                    A ferramenta ExecutaCodigo pode executar uma linha de codigo pandas com os dataframes mencionados anteriormente. Lá você manipula esses dataframes utilizando apenas a biblioteca pandas(pd).

                    A ferramenta ExportaDataframe é utilizada para exportar o df_final que foi gerado e que esta armazenado no self de uma classe python.
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
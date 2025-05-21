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
import os


class Agent:
    def __init__(self):
        llm = Anthropic(model="claude-3-5-sonnet-latest", temperature=0, timeout=None, max_retries=2)
        # llm = OpenAI(model="gpt-4.1-mini", temperature=0.5, max_retries=2)
        
        Settings.llm = llm
        
        self.tools = None
    
        self.colab = pd.read_excel("data/Dados Colaboradores.xlsx")
        self.unimed = pd.read_excel("data/Beneficio 1 - Unimed.xlsx")
        self.gympass = pd.read_excel("data/Beneficio 2 - Gympass.xlsx")
        self.github = pd.read_excel("data/Ferramenta 1 - Github.xlsx")
        self.google = pd.read_excel("data/Ferramenta 2 - Google workspace.xlsx")
        
        self._set_tools()
        self._init_agent()
        
    def _set_tools(self):

        pandas_executor = PandasExecutor(self.colab, self.unimed, self.gympass, self.github, self.google)
        
        get_infos = FunctionTool.from_defaults(
            pandas_executor.get_infos,
            name="GetInfos",
            description="Funcao que retorna as informacoes dos dataframes. Essa funcao deve ser chamada antes de executar qualquer linha de codigo."
        )

        exec_code = FunctionTool.from_defaults(
            pandas_executor.execute,
            name="ExecutaCodigo",
            description="Funcao capaz de executar um codigo pandas em dataframes. OBRIGATÓRIO: A funcao DEVE receber o parametro 'codigo', uma string contendo o codigo a ser executado. Exemplo de uso correto: ExecutaCodigo(codigo='df_final = colab.copy()'). Apenas a biblioteca pandas(pd) esta disponivel no ambiente."
        )

        export_xlsx = FunctionTool.from_defaults(
            pandas_executor.export_df,
            name="ExportaDataframe",
            description="Funcao capaz de exportar o dataframe final."
        )
    
        
        self.tools = [get_infos, exec_code, export_xlsx]        
        
        
    def _init_agent(self):
        self.agent = FunctionAgent(
            tools=self.tools,
            system_prompt=(
                f"""
                    Você é um agente especializado em análise de dados. Você tem a capacidade de manipular alguns dataframes através de funções.
                                        
                    Através das suas ferramentas você pode ler e manipular os dados dos dataframes.
                    
                    IMPORTANTE: A ferramenta ExecutaCodigo DEVE receber exatamente um argumento chamado 'codigo'. Por exemplo:
                    ExecutaCodigo(codigo="df_final = colab[['Nome', 'CPF']].copy()")
                    
                    Nunca chame ExecutaCodigo sem fornecer o argumento 'codigo'. A sintaxe ExecutaCodigo() sem argumentos resultará em erro.

                    A ferramenta ExportaDataframe é utilizada para exportar o df_final que foi gerado e que esta armazenado no self de uma classe python.
                            
                    Seu trabalho final é exportar um dataframe com as seguintes colunas: ID, Nome, Centro de Custo, Custo por Ferramenta, Custo por Beneficio e Custo Total. Levando em consideração os custos de todas as planilhas que você possui.
                    
                    Fluxo de trabalho recomendado:
                    1. Use GetInfos() para obter informações sobre os dataframes
                    2. Use ExecutaCodigo(codigo="seu_codigo_aqui") para processar os dados
                    3. Use ExportaDataframe() para exportar o resultado final
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
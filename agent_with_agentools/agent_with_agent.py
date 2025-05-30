import asyncio
from dotenv import load_dotenv

import pandas as pd

from agent_with_agentools.tool_agents_controller import ToolAgentsController
load_dotenv()

from llama_index.core.agent.workflow import FunctionAgent
from llama_index.core.tools import FunctionTool
from llama_index.core.agent.workflow import ToolCallResult, AgentStream


from llama_index.llms.anthropic import Anthropic
from llama_index.llms.openai import OpenAI

from llama_index.core import Settings
import os


class Agent:
    def __init__(self):
        # llm = Anthropic(model="claude-3-5-haiku-latest", temperature=0, timeout=None, max_retries=2)
        llm = OpenAI(model="gpt-4.1-mini", temperature=0.5, max_retries=2)
        
        Settings.llm = llm
        
        self.tools = None
        
        data_path = "data"
        
        self.dataframes = {}
        
        for file in os.listdir(data_path):
            if file.endswith(".xlsx"):
                file_path = os.path.join(data_path, file)
                self.dataframes[file.replace(".xlsx", "").lower().replace("-", "_").replace(" ", "")] = pd.read_excel(file_path)
                
        self.dataframes["df_final"] = pd.DataFrame()
        
        self._set_tools()
        self._init_agent()
        
    def _set_tools(self):

        tool_agents = ToolAgentsController(self.dataframes)
        
        get_infos = FunctionTool.from_defaults(
            tool_agents.get_infos,
            name="GetInfos",
            description="Funcao que retorna as informacoes dos dataframes. Essa funcao deve ser chamada antes de executar qualquer linha de codigo. Você pode chama-la multiplas vezes para verificar o estado dos dataframes. Ela lista o nome e informações dos dataframes que estão disponíveis para manipulação.",
        )

        generate_unique_id = FunctionTool.from_defaults(
            tool_agents.generate_unique_id,
            name="GenerateUniqueID",
            description="""Agente que gera um id unico para cada colaborador.
            A coluna resultante deve ser utilizada como id unico e para operações de merge nos dataframes. 
            Deve ser passado uma lista com os nomes dos dataframes, uma lista com so nomes da colunas do nome do colaborador e uma lista com os nomes das colunas do documento do colaborador para os respectivos dataframes.
            Essa função DEVE ser chamada logo após a execução da função GetInfos, para garantir que os dataframes estejam prontos para serem manipulados.
            gera_id_unico(self, dataframes: list, nome_coluna_nome: list, nome_coluna_documento: list)""",
        )

        rename_column_agent = FunctionTool.from_defaults(
            tool_agents.rename_column_agent,
            name="RenameColumnsAgent",
            description="""Agente capaz de renomear as colunas de um dataframe. Voce deve passar os seguintes parametros:
            rename_columns(self, dataframe_to_change: str, columns: list, new_columns: list)
            """,
        )

        remove_columns_agent = FunctionTool.from_defaults(
            tool_agents.remove_columns_agent,
            name="RemoveColumnsAgent",
            description="""Agente capaz de remover colunas de um dataframe. Voce deve passar os seguintes parametros:
            remove_columns(self, dataframe_to_change: str, columns: list)
            """,
        )

        select_columns_agent = FunctionTool.from_defaults(
            tool_agents.select_columns_agent,
            name="SelectColumnsAgent",
            description="""Agente capaz de selecionar colunas de um dataframe e modifica-lo. Voce deve passar os seguintes parametros:
            select_columns(self, dataframe: str, columns: list)
            """,
        )

        sum_columns_agent = FunctionTool.from_defaults(
            tool_agents.sum_columns_agent,
            name="SumColumnsAgent",
            description="""Agente capaz de somar colunas de um dataframe. Voce deve passar os seguintes parametros:
            soma_colunas(self, dataframe: str, columns: list, new_column_name: str)
            """,
        )

        merge_dataframes_agent = FunctionTool.from_defaults(
            tool_agents.merge_dataframes_agent,
            name="MergeDataframesAgent",
            description="""Agente capaz de mesclar dataframes. Voce deve passar os seguintes parametros: 
            merge_dataframes(self, dataframe1: str, dataframe2: str, left_on: str, right_on: str, how: str, destination: str)
            """,
        )

        export_xlsx = FunctionTool.from_defaults(
            tool_agents.export_df,
            name="ExportaDataframe",
            description="""Funcao capaz de exportar o dataframe final. Deve ser passado os seguintes parametros: 
            export_df(self, dataframe_name: str)
            """
        )

        token_counts = FunctionTool.from_defaults(
            tool_agents.print_token_count,
            name="GetTokenCounts",
            description="""Funcao que retorna a contagem de tokens utilizados na ultima execucao do agente.
            Executar essa função após exportar o dataframe. 
            Retorna um dicionario com as chaves 'prompt_tokens' e 'completion_tokens'.
            """,
        )
            

        self.tools = [get_infos, generate_unique_id, rename_column_agent, remove_columns_agent, select_columns_agent, sum_columns_agent, merge_dataframes_agent, export_xlsx, token_counts]        
        
        
    def _init_agent(self):
        self.agent = FunctionAgent(
            tools=self.tools,
            system_prompt=(
                f"""
                    Você é um agente especializado em análise de dados. Você tem a capacidade de manipular alguns dataframes através outros agents que utilizam suas proprias ferramentas.
                    As manipulacoes de ferramentas desses agents são compostas por códigos Pandas, e você pode utilizá-las para realizar operações como renomear colunas, remover colunas, selecionar colunas, somar colunas, mesclar dataframes e exportar dataframes para arquivos Excel.
                                        
                    Voce possui a ferramenta GetInfos para obter informações dos dataframes, que retorna o nome e informações dos dataframes que estão disponíveis para manipulação.

                    A ferramenta GenerateUniqueID é uma função que gera um id único para cada colaborador em todos os dataframes que voce esta manipulando. gera_id_unico(self, dataframes: list, nome_coluna_nome: list, nome_coluna_documento: list). Essa função DEVE ser chamada logo após a execução da função GetInfos, para garantir que os dataframes estejam prontos para serem manipulados. 
                    
                    Voce possui a ferramenta RenameColumnsAgent, que é um agente capaz de renomear as colunas de um dataframe. rename_columns(self, dataframe_to_change: str, columns: list, new_columns: list)

                    A ferramenta RemoveColumnsAgent é um agente capaz de remover colunas de um dataframe. remove_columns(self, dataframe_to_change: str, columns: list)

                    A ferramenta SelectColumnsAgent é um agente capaz de selecionar colunas de um dataframe e modifica-lo. Muito util para quando estiver obtendo erros ao tentar remover colunas. select_columns(self, dataframe: str, columns: list)

                    A ferramenta SumColumnsAgent é um agente capaz de somar colunas de um dataframe. soma_colunas(self, dataframe: str, columns: list, new_column_name: str)

                    A ferramenta MergeDataframesAgent é um agente capaz de mesclar dataframes. merge_dataframes(self, dataframe1: str, dataframe2: str, left_on: str, right_on: str, how: str, destination: str)

                    A ferramenta ExportaDataframe é utilizada para exportar um dataframe para um arquivo Excel. export_df(self, dataframe_to_export: str)

                    A ferramenta GetTokenCounts é uma função que retorna a contagem de tokens utilizados a cada function call. Executar essa função após exportar o dataframe. Retorna um dicionario com as chaves 'prompt_tokens' e 'completion_tokens'.

                """
            )
        )
        
        
                
    async def run(self, query):
        
        handler = self.agent.run(query)

        async for ev in handler.stream_events():
            if isinstance(ev, ToolCallResult):
                print(
                    f"Mother agent Call {ev.tool_name} with args {ev.tool_kwargs}\nReturned: {ev.tool_output}"
                )
            # elif isinstance(ev, AgentStream):
            #     print(ev.delta, end="", flush=True)

        response = await handler
        
        return response
    
    
# if __name__ == "__main__":
#     agent = Agent()
    
#     asyncio.run(agent.run("""
#         gere e exporte um dataframe que calcula os custos por colaborador e exporte-o APENAS com colunas: 
#             -ID: identificador do colaborador (id unico)
#             -Nome: Nome do colaborador
#             -Centro de Custo: centro de custo do colaborador
#             -uma coluna para cada custo, sendo o nome da coluna o nome do custo (incluindo salario)
#             -Custo Total: soma de todos os custos do colaborador
            
#         Não insira mais nenhuma coluna relacionada a outras coisas no dataframe final.
#     """))

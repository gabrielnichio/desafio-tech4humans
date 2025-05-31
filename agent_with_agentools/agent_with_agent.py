import asyncio
from dotenv import load_dotenv

import pandas as pd
from fastapi import UploadFile

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
        llm = Anthropic(model="claude-3-5-haiku-latest", temperature=0, timeout=None, max_retries=2)
        # llm = OpenAI(model="gpt-4.1-mini", temperature=0.5, max_retries=2)
        
        Settings.llm = llm
        
        self.tools = None

        self.tool_agents = None
                
        self.dataframes = {}

    
    def set_dataframes(self, dataframes: list[UploadFile]):
        
        for dataframe in dataframes:
            if dataframe.filename.replace(".xlsx", "").lower().replace("-", "_").replace(" ", "") in self.dataframes.keys():
                continue
            self.dataframes[dataframe.filename.replace(".xlsx", "").lower().replace("-", "_").replace(" ", "")] = pd.read_excel(dataframe.file)
                
        self.dataframes["df_final"] = pd.DataFrame()

        print("dataframes carregados:", self.dataframes.keys())

        self._set_tools()
        self._init_agent()

    def verify_dataframes(self):
        return len(self.dataframes.keys()) > 1    
    
    def html_analysis(self):
        return self.tool_agents.html_analysis("df_final")

    def _set_tools(self):

        self.tool_agents = ToolAgentsController(self.dataframes)
        
        get_infos = FunctionTool.from_defaults(
            self.tool_agents.get_infos,
            name="GetInfos",
            description="Funcao que retorna as informacoes dos dataframes. Essa funcao deve ser chamada antes de executar qualquer linha de codigo. Você pode chama-la multiplas vezes para verificar o estado dos dataframes. Ela lista o nome e informações dos dataframes que estão disponíveis para manipulação.",
        )

        generate_unique_id = FunctionTool.from_defaults(
            self.tool_agents.generate_unique_id,
            name="GenerateUniqueID",
            description="""Agente que gera um id unico para cada colaborador.
            A coluna resultante deve ser utilizada como id unico e para operações de merge nos dataframes. 
            gera_id_unico(self, dataframes: list, nome_coluna_nome: list, nome_coluna_documento: list)""",
        )

        rename_column_agent = FunctionTool.from_defaults(
            self.tool_agents.rename_column_agent,
            name="RenameColumnsAgent",
            description="""Agente capaz de renomear as colunas de um dataframe. Voce deve passar os seguintes parametros:
            rename_columns(self, dataframe_to_change: str, columns: list, new_columns: list)
            """,
        )

        rename_multiple_df_columns_agent = FunctionTool.from_defaults(
            self.tool_agents.rename_multiple_dataframes_columns_agent,
            name="RenameMultipleDfColumnsAgent",
            description="""Agente capaz de renomear multiplos dataframes. Voce deve passar os seguintes parametros:
            rename_multiple_df_columns(self, dataframes: dict)
            """,
        )

        remove_columns_agent = FunctionTool.from_defaults(
            self.tool_agents.remove_columns_agent,
            name="RemoveColumnsAgent",
            description="""Agente capaz de remover colunas de um dataframe. Voce deve passar os seguintes parametros:
            remove_columns(self, dataframe_to_change: str, columns: list)
            """,
        )

        select_columns_agent = FunctionTool.from_defaults(
            self.tool_agents.select_columns_agent,
            name="SelectColumnsAgent",
            description="""Agente capaz de selecionar colunas de um dataframe e modifica-lo. Voce deve passar os seguintes parametros:
            select_columns(self, dataframe: str, columns: list)
            """,
        )

        select_multiple_df_columns_agent = FunctionTool.from_defaults(
            self.tool_agents.select_multiple_df_columns_agent,
            name="SelectMultipleDfColumnsAgent",
            description="""Agente capaz de selecionar colunas de multiplos dataframes. Voce deve passar os seguintes parametros:
            select_multiple_df_columns(self, dataframes: dict)
            """
        )

        sum_columns_agent = FunctionTool.from_defaults(
            self.tool_agents.sum_columns_agent,
            name="SumColumnsAgent",
            description="""Agente capaz de somar colunas de um dataframe. Voce deve passar os seguintes parametros:
            soma_colunas(self, dataframe: str, columns: list, new_column_name: str)
            """,
        )

        sum_multiple_columns_agent = FunctionTool.from_defaults(
            self.tool_agents.sum_multiple_columns_agent,
            name="SumMultipleColumnsAgent",
            description="""Agente capaz de somar multiplos dataframes. Voce deve passar os seguintes parametros:
            sum_column_groups(self, dataframe: str, groups: dict)
            """
        )

        merge_dataframes_agent = FunctionTool.from_defaults(
            self.tool_agents.merge_dataframes_agent,
            name="MergeDataframesAgent",
            description="""Agente capaz de mesclar dataframes. Voce deve passar os seguintes parametros: 
            merge_dataframes(self, dataframe1: str, dataframe2: str, left_on: str, right_on: str, how: str, destination: str)
            """,
        )

        merge_multiple_dataframes_agent = FunctionTool.from_defaults(
            self.tool_agents.merge_multiple_dataframes_agent,
            name="MergeMultipleDataframesAgent",
            description="""Agente capaz de mesclar multiplos dataframes. Voce deve passar os seguintes parametros:
            merge_multiple_dataframes(self, dataframes_list: list, on_column: str, how: str, destination: str)
            """
        )
        export_xlsx = FunctionTool.from_defaults(
            self.tool_agents.export_df,
            name="ExportaDataframe",
            description="""Funcao capaz de exportar o dataframe final. Deve ser passado os seguintes parametros: 
            export_df(self, dataframe_name: str)
            """
        )            

        self.tools = [
            get_infos,
            generate_unique_id,
            rename_column_agent,
            rename_multiple_df_columns_agent,
            remove_columns_agent,
            select_columns_agent,
            select_multiple_df_columns_agent,
            sum_columns_agent,
            sum_multiple_columns_agent,
            merge_dataframes_agent,
            merge_multiple_dataframes_agent,
            export_xlsx
        ]        
        
        
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

                    A ferramenta RenameMultipleDfColumnsAgent é um agente capaz de renomear multiplos dataframes. rename_multiple_df_columns(self, dataframes: dict)

                    A ferramenta RemoveColumnsAgent é um agente capaz de remover colunas de um dataframe. remove_columns(self, dataframe_to_change: str, columns: list)

                    A ferramenta SelectColumnsAgent é um agente capaz de selecionar colunas de um dataframe e modifica-lo. Muito util para quando estiver obtendo erros ao tentar remover colunas. select_columns(self, dataframe: str, columns: list)

                    A ferramenta SelectMultipleDfColumnsAgent é um agente capaz de selecionar colunas de multiplos dataframes. select_multiple_df_columns(self, dataframes: dict)

                    A ferramenta SumColumnsAgent é um agente capaz de somar colunas de um dataframe. soma_colunas(self, dataframe: str, columns: list, new_column_name: str)

                    A ferramenta SumMultipleColumnsAgent é um agente capaz de somar multiplos dataframes. É uma função util para agilizar o fluxo e economizar consumo quando voce quer somar varias colunas de diversos dataframes de uma vez. sum_column_groups(self, dataframe: str, groups: dict)

                    A ferramenta MergeDataframesAgent é um agente capaz de mesclar dataframes. merge_dataframes(self, dataframe1: str, dataframe2: str, left_on: str, right_on: str, how: str, destination: str)

                    Além disso, você possui a ferramenta MergeMultipleDataframesAgent, que é um agente capaz de mesclar multiplos dataframes. É uma função útil para agilizar o fluxo e economizar consumo quando voce quer e dar merge em varios dataframes de uma vez. merge_multiple_dataframes(self, dataframes_list: list, on_column: str, how: str, destination: str)
                    
                    A ferramenta ExportaDataframe é utilizada para exportar um dataframe para um arquivo Excel. export_df(self, dataframe_to_export: str)

                    Coloque e exporte o dataframe final com o nome 'df_final'.

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

        response = await handler
        
        return response

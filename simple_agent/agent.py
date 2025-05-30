import asyncio
from dotenv import load_dotenv

import pandas as pd
from fastapi import UploadFile

from simple_agent.exec_func import PandasExecutor
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
        
    def _set_tools(self):

        pandas_executor = PandasExecutor(self.dataframes)
        
        get_infos = FunctionTool.from_defaults(
            pandas_executor.get_infos,
            name="GetInfos",
            description="Funcao que retorna as informacoes dos dataframes. Essa funcao deve ser chamada antes de executar qualquer linha de codigo. Você pode chama-la multiplas vezes para verificar o estado dos dataframes. Ela lista o nome e informações dos dataframes que estão disponíveis para manipulação.",
        )

        gera_id_unico = FunctionTool.from_defaults(
            pandas_executor.gera_id_unico,
            name="GeraIDUnico",
            description="""
                Funcao que gera um id unico para cada colaborador em todos os dataframes. A coluna resultante deve ser utilizada como id unico e para operações de merge nos dataframes. 
                Deve ser passado uma lista com os nomes dos dataframes, uma lista com so nomes da colunas do nome do colaborador e uma lista com os nomes das colunas do documento do colaborador para os respectivos dataframes.
                Essa função DEVE ser chamada logo após a execução da função GetInfos, para garantir que os dataframes estejam prontos para serem manipulados.
                Exemplo: GeraIDUnico(dataframes=['df1', 'df2'], nome_coluna_nome=['coluna1', 'coluna2'], nome_coluna_documento=['coluna3', 'coluna4'])
            """
        )
        
        rename_columns = FunctionTool.from_defaults(
            pandas_executor.rename_columns,
            name="RenameColumns",
            description="Função que renomeia as colunas de um dataframe. rename_columns(self, dataframe_to_change: str, columns: list, new_columns: list). Exemplo: RenameColumns(dataframe='df1', columns=['coluna1', 'coluna2'], new_columns=['nova_coluna1', 'nova_coluna2'])",
        )

        rename_multiple_columns = FunctionTool.from_defaults(
            pandas_executor.rename_multiple_dataframe_columns,
            name="RenameMultipleDfColumns",
            description="Função que renomeia várias colunas de varios dataframes diferentes. rename_multiple_df_columns(self, dataframes: dict) Exemplo: RenameMultipleDfColumns(dataframes={'dataframe1': {'previous_names': ['col1', 'col2'], 'new_names': ['nova_col1', 'nova_col2']}, 'dataframe2': {'previous_names': ['col3'], 'new_names': ['nova_col3']})"
        )
        
        remove_colunas = FunctionTool.from_defaults(
            pandas_executor.remove_columns,
            name="RemoveColumns",
            description="Funcao que remove as colunas de um dataframe. Deve ser passado o nome do dataframe e as colunas a serem removidas. Exemplo: RemoveColumns(dataframe='df1', columns=['coluna1', 'coluna2'])"
        )
        
        selecionar_colunas = FunctionTool.from_defaults(
            pandas_executor.select_columns,
            name="SelecionarColunas",
            description="Funcao que seleciona as colunas de um dataframe. Deve ser passado o nome do dataframe e as colunas a serem selecionadas. Exemplo: SelecionarColunas(dataframe='df1', columns=['coluna1', 'coluna2'])"
        )

        select_multiple_df_columns = FunctionTool.from_defaults(
            pandas_executor.select_multiple_df_columns,
            name="SelectMultipleDfColumns",
            description="Funcao que seleciona as colunas de varios dataframes diferentes. select_multiple_df_columns(self, dataframes: dict). Exemplo: SelecionarColunasMultiploDf(dataframes={'dataframe1': ['coluna1', 'coluna2'], 'dataframe2': ['coluna3']})"
        )
        
        soma_colunas = FunctionTool.from_defaults(
            pandas_executor.soma_colunas,
            name="SomaColunas",
            description="Funcao que soma as colunas de um dataframe. Deve ser passado o nome do dataframe, as colunas a serem somadas e o nome da nova coluna. Exemplo: SomaColunas(dataframe='df1', columns=['coluna1', 'coluna2'], new_column_name='soma')"
        )
        
        
        soma_group_columns = FunctionTool.from_defaults(
            pandas_executor.sum_column_groups,
            name="SomaColunasAgrupadas",
            description="Função que soma varios grupos de colunas de um dataframe de uma só vez. Deve ser passado o nome do dataframe e um dicionario onde as chaves são os nomes das novas colunas e os valores são listas com os nomes das colunas a serem somadas. Exemplo: SomaColunasAgrupadas(dataframe='df1', column_groups={'soma1': ['coluna1', 'coluna2'], 'soma2': ['coluna3', 'coluna4']})"
        )

        merge_dataframes = FunctionTool.from_defaults(
            pandas_executor.merge_dataframes,
            name="MergeDataframes",
            description="Função que faz o merge de dois dataframes. Deve ser passado o nome dos dataframes a serem unidos, a coluna do dataframe da esquerda, coluna do dataframe da direita, o parametro how do merge, e o nome do dataframe de destino. Nunca utilize nomes de dataframes que não existam. Exemplo: MergeDataframes(dataframe1='df1', dataframe2='df2', left_on='coluna_df1', right_on='coluna_df2', how='left', destination='df_final')",
        )
        
        merge_multiple_dfs = FunctionTool.from_defaults(
            pandas_executor.merge_multiple_dataframes,
            name="MergeMultipleDataframes",
            description="Função que faz o merge de múltiplos dataframes usando uma coluna em comum. Deve ser passado uma lista com os nomes dos dataframes a serem unidos, o nome da coluna a ser usada como base para o merge (geralmente 'id_unico'), o parâmetro how do merge ('left', 'right', 'inner', 'outer'), e o nome do dataframe de destino. Exemplo: MergeMultipleDataframes(dataframes_list=['df1', 'df2', 'df3'], on_column='id_unico', how='left', destination='df_final')",
        )
        
        export_xlsx = FunctionTool.from_defaults(
            pandas_executor.export_df,
            name="ExportaDataframe",
            description="Funcao capaz de exportar o dataframe final. Deve ser passado um parâmetro de string especificando o nome do dataframe a ser exportado."
        )
    
        
        self.tools = [
            get_infos, 
            gera_id_unico, 
            rename_columns, 
            rename_multiple_columns,
            selecionar_colunas, 
            select_multiple_df_columns,
            soma_colunas,
            soma_group_columns, 
            merge_dataframes,
            merge_multiple_dfs, 
            export_xlsx
        ]        
        
        
    def _init_agent(self):
        self.agent = FunctionAgent(
            tools=self.tools,
            system_prompt=(
                f"""
                    Você é um agente especializado em análise de dados. Você tem a capacidade de manipular alguns dataframes através de funções.
                                        
                    Através das suas ferramentas você pode ler e manipular os dados dos dataframes.
                    
                    Com a ferramenta GetInfos você pode obter informações sobre os dataframes disponíveis para manipulação. Ela deve ser chamada antes de executar qualquer outra função para você saber a estrutura dos dataframes disponíveis. Você pode chama-la multiplas vezes para verificar o estado dos dataframes.
                    
                    Com a ferramenta TransformDuplicateValues você pode transformar valores duplicados em uma coluna de um dataframe. Utilize essa funcao apenas em colunas e dataframes que fazem sentido para operacoes futuras. Deve ser passado o nome do dataframe e a coluna a ser transformada. Exemplo: TransformDuplicateValues(dataframe='df1', column='coluna1')

                    A ferramenta GeraIDUnico é utilizada para gerar um id unico para cada colaborador em todos os dataframes. A coluna resultante deve ser utilizada como id unico para operações nos dataframes. 

                    A ferramenta RenameColumns é utilizada para renomear as colunas de um dataframe. rename_columns(self, dataframe_to_change: str, columns: list, new_columns: list)

                    A ferramenta RenameMultipleDfColumns é utilizada para renomear várias colunas de vários dataframes diferentes. Util para quando você puder maniuplar varios dataframes de uma vez para economizar tempo e recursos. rename_multiple_dataframe_columns(self, dataframes: dict) 
                                    
                    A ferramenta SelecionarColunas é utilizada para selecionar as colunas de um dataframe. select_columns(self, dataframe: str, columns: list)
                    
                    A ferramenta SelectMultipleDfColumns é utilizada para selecionar as colunas de vários dataframes diferentes. select_multiple_df_columns(self, dataframes: dict)

                    A ferramenta SomaColunas é utilizada para somar as colunas de um dataframe. soma_colunas(self, dataframe: str, columns: list, new_column_name: str).
                    
                    A ferramenta SomaColunasAgrupadas é utilizada para somar vários grupos de colunas de um dataframe de uma só vez. sum_column_groups(self, dataframe: str, groups: dict)

                    A ferramenta MergeDataframes é utilizada para fazer o merge de dois dataframes. merge_dataframes(self, dataframe1: str, dataframe2: str, left_on: str, right_on: str, how: str, destination: str).
                    
                    A ferramenta MergeMultipleDataframes é utilizada para fazer o merge de múltiplos dataframes de uma só vez usando uma coluna em comum. merge_dataframes(self, dataframe1: str, dataframe2: str, left_on: str, right_on: str, how: str, destination: str)

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
                    f"Call {ev.tool_name} with args {ev.tool_kwargs}\nReturned: {ev.tool_output}"
                )
            elif isinstance(ev, AgentStream):
                print(ev.delta, end="", flush=True)

        response = await handler
        
        print(f"Resposta: ${response}")
        return response

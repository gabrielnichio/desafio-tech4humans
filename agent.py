import asyncio
from dotenv import load_dotenv

import pandas as pd

from exec_func import PandasExecutor
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
        llm = Anthropic(model="claude-3-5-haiku-latest", temperature=0, timeout=None, max_retries=2)
        # llm = OpenAI(model="gpt-4.1-mini", temperature=0.5, max_retries=2)
        
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

        pandas_executor = PandasExecutor(self.dataframes)
        
        get_infos = FunctionTool.from_defaults(
            pandas_executor.get_infos,
            name="GetInfos",
            description="Funcao que retorna as informacoes dos dataframes. Essa funcao deve ser chamada antes de executar qualquer linha de codigo. Ela lista o nome e informações dos dataframes que estão disponíveis para manipulação.",
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
            description="Função que renomeia as colunas de um dataframe. Deve ser passado o nome do dataframe, as colunas a serem renomeadas e os novos nomes das colunas. Exemplo: RenameColumns(dataframe='df1', columns=['coluna1', 'coluna2'], new_columns=['nova_coluna1', 'nova_coluna2'])",
        )
        
        remove_colunas = FunctionTool.from_defaults(
            pandas_executor.remove_columns,
            name="RemoveColumns",
            description="Funcao que remove as colunas de um dataframe. Deve ser passado o nome do dataframe e as colunas a serem removidas. Exemplo: RemoveColumns(dataframe='df1', columns=['coluna1', 'coluna2'])"
        )
        
        criar_dataframe = FunctionTool.from_defaults(
            pandas_executor.criar_dataframe,
            name="CriaDataframe",
            description="Funcao que cria um novo dataframe. Deve ser passado o nome do dataframe e as colunas do dataframe. Exemplo: CriaDataframe(dataframe='df1', columns=['coluna1', 'coluna2'])"
        )
        
        soma_colunas = FunctionTool.from_defaults(
            pandas_executor.soma_colunas,
            name="SomaColunas",
            description="Funcao que soma as colunas de um dataframe. Deve ser passado o nome do dataframe, as colunas a serem somadas e o nome da nova coluna. Exemplo: SomaColunas(dataframe='df1', columns=['coluna1', 'coluna2'], new_column_name='soma')"
        )
        
        merge_dataframes = FunctionTool.from_defaults(
            pandas_executor.merge_dataframes,
            name="MergeDataframes",
            description="Função que faz o merge de dois dataframes. Deve ser passado o nome dos dataframes a serem unidos, a coluna do dataframe da esquerda, coluna do dataframe da direita (Utilize o nome nos dataframes como base para o merge), o parametro how do merge, e o nome do dataframe de destino. Nunca utilize nomes de dataframes que não existam. Exemplo: MergeDataframes(dataframe1='df1', dataframe2='df2', left_on='coluna_df1', right_on='coluna_df2', how='left', destination='df_final')",
        )
        
        export_xlsx = FunctionTool.from_defaults(
            pandas_executor.export_df,
            name="ExportaDataframe",
            description="Funcao capaz de exportar o dataframe final. Deve ser passado um parâmetro de string especificando o nome do dataframe a ser exportado."
        )
    
        
        self.tools = [get_infos, gera_id_unico, rename_columns, remove_colunas, soma_colunas, merge_dataframes, export_xlsx]        
        
        
    def _init_agent(self):
        self.agent = FunctionAgent(
            tools=self.tools,
            system_prompt=(
                f"""
                    Você é um agente especializado em análise de dados. Você tem a capacidade de manipular alguns dataframes através de funções.
                                        
                    Através das suas ferramentas você pode ler e manipular os dados dos dataframes.
                    
                    Com a ferramenta GetInfos você pode obter informações sobre os dataframes disponíveis para manipulação. Ela deve ser chamada antes de executar qualquer outra função para você saber a estrutura dos dataframes disponíveis. A funcao tambem mostra colunas com valores repetidos, leve isso em consideracao para escolher as colunas que serao utilizadas no merge.
                    
                    A ferramenta GeraIDUnico é utilizada para gerar um id unico para cada colaborador em todos os dataframes. A coluna resultante deve ser utilizada como id unico e para operações de merge nos dataframes. Você deve passar uma lista com os nomes dos dataframes, uma lista com so nomes da colunas do nome do colaborador e uma lista com os nomes das colunas do documento do colaborador para os respectivos dataframes. Essa função DEVE ser chamada logo após a execução da função GetInfos, para garantir que os dataframes estejam prontos para serem manipulados.

                    A ferramenta RenameColumns é utilizada para renomear as colunas de um dataframe. Você deve passar o nome do dataframe, as colunas a serem renomeadas e os novos nomes das colunas. Utilize ela em todos os dataframes necessários para padronizar o nome das colunas em comum em todos os dataframes. Você pode usar essa ferramenta quantas vezes for necessário para renomear as colunas do dataframe.
                                   
                    A ferramenta RemoveColumns é utilizada para remover as colunas de um dataframe. Você deve passar o nome do dataframe e as colunas a serem removidas. Utilize ela em todos os dataframes necessários para remover as colunas que não são necessárias para o seu trabalho. Você pode usar essa ferramenta quantas vezes for necessário para remover as colunas do dataframe.     
                    
                    A ferramenta SomaColunas é utilizada para somar as colunas de um dataframe. Você deve passar o nome do dataframe, as colunas a serem somadas e o nome da nova coluna. Ela pode ser utilizada quantas vezes for necessário para somar as colunas do dataframe.
                                        
                    A ferramenta MergeDataframes é utilizada para fazer o merge de dois dataframes. Você deve passar o nome dos dataframes a serem unidos, a coluna do dataframe da esquerda, coluna do dataframe da direita, o parametro how do merge e o nome do dataframe de destino. Ela pode ser utilizada quantas vezes for necessário para unir os dataframes.
                    Dê merge utilizando como base as colunas de nome nos dataframes.
                    NUNCA utilize nomes de dataframes que não existem como parâmetro para as funções.

                    A ferramenta ExportaDataframe é utilizada para exportar um dataframe para um arquivo Excel. Você deve passar o nome do dataframe a ser exportado.
                    
                    - Considerações importantes:
                      - Sempre crie um novo dataframe antes de passar um nome novo para as outras funções.
                      - Utilize apenas os dataframes disponíveis para manipulação.
                      - Não passe nomes de dataframes que não existem como parâmetro para as funções.
                      
                    Fluxo de trabalho recomendado:
                    1. Chame a ferramenta GetInfos para obter informações sobre os dataframes disponíveis.
                    2. Utilize outras ferramentas para manipular os dataframes conforme necessário.
                    3. Chame a ferramenta GetInfos novamente para verificar o estado do processo.
                    4. Repita os passos 2 e 3 até que o dataframe final esteja pronto.
                    5. Exporte o dataframe final utilizando a ferramenta ExportaDataframe.
                    
                    Seu trabalho final é exportar um dataframe contendo informações sobre os colaboradores e seus custos.
                    
                    O dataframe final DEVE possuir SOMENTE as seguintes colunas: 
                        -ID: identificador do colaborador (documento do colaborador)
                        -Nome: Nome do colaborador
                        -Centro de Custo: centro de custo do colaborador
                        -Custo por Ferramenta: custo total das FERRAMENTAS utilizadas pelo colaborador
                        -Custo por Beneficio: custo total dos BENEFICIOS utilizados pelo colaborador
                        -Custo Total: soma de todos os custos do colaborador (ferramentas + beneficios + salario, + ...))
                    Levando em consideração os custos de todas as planilhas que você possui.
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
    
    # gere e exporte um dataframe que calcula os custos por colaborador e exporte-o com as colunas: ID, Nome, Centro de Custo, Custo por Ferramenta, Custo por Beneficio e Custo Total
    
if __name__ == "__main__":
    agent = Agent()
    
    asyncio.run(agent.run("gere e exporte um dataframe que calcula os custos por colaborador e exporte-o com as colunas: ID, Nome, Centro de Custo, Custo por Ferramenta, Custo por Beneficio e Custo Total"))

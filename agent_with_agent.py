import asyncio
from dotenv import load_dotenv

import pandas as pd

from tool_agents_controller import ToolAgentsController
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
            description="Funcao que gera um id unico para cada colaborador. Passe uma query que descreva o que você deseja fazer, e o agente irá gerar um id unico para cada colaborador nos dataframes especificados.",
        )

        rename_column_agent = FunctionTool.from_defaults(
            tool_agents.rename_column_agent,
            name="RenameColumnsAgent",
            description="Agente capaz de renomear as colunas de um dataframe. Voce deve passar uma query que descreva o que você deseja fazer, e o agente irá renomear as colunas do dataframe de acordo com a query.",
        )

        remove_columns_agent = FunctionTool.from_defaults(
            tool_agents.remove_columns_agent,
            name="RemoveColumnsAgent",
            description="Agente capaz de remover colunas de um dataframe. Voce deve passar uma query que descreva o que você deseja fazer, e o agente irá remover as colunas do dataframe de acordo com a query.",
        )

        sum_columns_agent = FunctionTool.from_defaults(
            tool_agents.sum_columns_agent,
            name="SumColumnsAgent",
            description="Agente capaz de somar colunas de um dataframe. Voce deve passar uma query que descreva o que você deseja fazer, e o agente irá somar as colunas do dataframe de acordo com a query.",
        )

        merge_dataframes_agent = FunctionTool.from_defaults(
            tool_agents.merge_dataframes_agent,
            name="MergeDataframesAgent",
            description="Agente capaz de mesclar dataframes. Voce deve passar uma query que descreva o que você deseja fazer, e o agente irá mesclar os dataframes de acordo com a query.",
        )

        export_xlsx = FunctionTool.from_defaults(
            tool_agents.export_df,
            name="ExportaDataframe",
            description="Funcao capaz de exportar o dataframe final. Deve ser passado um parâmetro de string especificando o nome do dataframe a ser exportado."
        )
            

        self.tools = [get_infos, generate_unique_id, rename_column_agent, remove_columns_agent, sum_columns_agent, merge_dataframes_agent, export_xlsx]        
        
        
    def _init_agent(self):
        self.agent = FunctionAgent(
            tools=self.tools,
            system_prompt=(
                f"""
                    Você é um agente especializado em análise de dados. Você tem a capacidade de manipular alguns dataframes através outros agents que sao ferramentas.
                                        
                    Voce possui a ferramenta GetInfos para obter informações dos dataframes, que retorna o nome e informações dos dataframes que estão disponíveis para manipulação.

                    A ferramenta GenerateUniqueID é uma função que gera um id único para cada colaborador em todos os dataframes que voce esta manipulando. Voce deve passar uma query especificando os dataframes, os nomes das colunas de nome e documento. Ex: GenerateUniqueID(query="especificacoes"). Essa função DEVE ser chamada logo após a execução da função GetInfos, para garantir que os dataframes estejam prontos para serem manipulados. 
                    
                    Voce possui a ferramenta RenameColumnsAgent, que é um agente capaz de renomear as colunas de um dataframe. Voce deve passar uma query especificando o nome do dataframe, as colunas a serem renomeadas e os novos nomes.

                    A ferramenta RemoveColumnsAgent é um agente capaz de remover colunas de um dataframe. Voce deve passar o nome do dataframe e a lista de colunas a serem removidas.

                    A ferramenta SumColumnsAgent é um agente capaz de somar colunas de um dataframe. Voce deve passar o nome do dataframe, as colunas a serem somadas e o novo nome da coluna.

                    A ferramenta MergeDataframesAgent é um agente capaz de mesclar dataframes. Deve ser passado o nome dos dataframes a serem unidos, a coluna do dataframe da esquerda, coluna do dataframe da direita, o parametro how do merge, e o nome do dataframe de destino. Nunca utilize nomes de dataframes que não existam. Exemplo: MergeDataframesAgent(dataframe1='df1', dataframe2='df2', left_on='coluna_df1', right_on='coluna_df2', how='left', destination='df_final')

                    A ferramenta ExportaDataframe é utilizada para exportar um dataframe para um arquivo Excel. Você deve passar o nome do dataframe a ser exportado.

                    Ao passar a query, especifique corretamente os nomes de todos os parametros que você deseja utilizar, e o agente irá executar as ações necessárias para atender à sua solicitação.
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
        
        print(f"Resposta: ${response}")
        return response
    
    
if __name__ == "__main__":
    agent = Agent()
    
    asyncio.run(agent.run("""
        gere e exporte um dataframe que calcula os custos por colaborador e exporte-o APENAS com colunas: 
            -ID: identificador do colaborador (id unico)
            -Nome: Nome do colaborador
            -Centro de Custo: centro de custo do colaborador
            -uma coluna para cada custo, sendo o nome da coluna o nome do custo
            -Custo Total: soma de todos os custos do colaborador
            
        Não insira mais nenhuma coluna relacionada a outras coisas no dataframe final.
    """))

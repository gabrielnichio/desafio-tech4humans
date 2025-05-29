from tool_agent import ToolAgent
from exec_func import PandasExecutor

class ToolAgentsController:
    def __init__(self, dataframes):
        self.pandas_executor = PandasExecutor(dataframes)

    def get_infos(self):
        return self.pandas_executor.get_infos()
    
    async def generate_unique_id(self, query):
        agent = ToolAgent(
            self.pandas_executor.gera_id_unico,
            tool_name="GenerateUniqueID",
            tool_description="Funcao que gera um id unico para cada colaborador. Voce deve passar os dataframes, os nomes das colunas de nome e documento.",
            agent_prompt=(
                """
                    Agente que gera um id unico para cada colaborador em todas as planilhas que voce esta manipulando.
                    Voce possui uma tool que recebe a lista dos dataframes, os nomes das colunas de nome e documento.
                """
            )
        )

        response = await agent.tool_agent(query)

        return response

    async def rename_column_agent(self, query):
        agent = ToolAgent(
            self.pandas_executor.rename_columns,
            tool_name="RenameColumnsAgent",
            tool_description="Agente capaz de renomear as colunas de um dataframe. Voce deve passar o nome do dataframe, as colunas a serem renomeadas e os novos nomes.",
            agent_prompt=(
                """
                    Agente que renomeia as colunas de um dataframe. 
                    Voce possui uma tool aque recebe o nome do dataframe, as colunas a serem renomeadas e os novos nomes.
                """
            )
        )

        response = await agent.tool_agent(query)

        return response
    
    async def remove_columns_agent(self, query):
        agent = ToolAgent(
            self.pandas_executor.remove_columns,
            tool_name="RemoveColumnsAgent",
            tool_description="Agente capaz de remover colunas de um dataframe. Voce deve passar o nome do dataframe e a lista de colunas a serem removidas.",
            agent_prompt=(
                """
                    Agente que remove colunas de um dataframe. 
                    Voce possui uma tool que recebe o nome do dataframe e a lista de colunas a serem removidas.
                """
            )
        )

        response = await agent.tool_agent(query)

        return response
    
    async def sum_columns_agent(self, query):
        agent = ToolAgent(
            self.pandas_executor.soma_colunas,
            tool_name="SumColumnsAgent",
            tool_description="Agente capaz de somar colunas de um dataframe. Voce deve passar o nome do dataframe, as colunas a serem somadas e o novo nome da coluna.",
            agent_prompt=(
                """
                    Agente que soma colunas de um dataframe. 
                    Voce possui uma tool que recebe o nome do dataframe, as colunas a serem somadas e o novo nome da coluna.
                """
            )
        )

        response = await agent.tool_agent(query)

        return response

    async def merge_dataframes_agent(self, query):
        agent = ToolAgent(
            self.pandas_executor.merge_dataframes,
            tool_name="MergeDataframesAgent",
            tool_description="Função que faz o merge de dois dataframes. Deve ser passado o nome dos dataframes a serem unidos, a coluna do dataframe da esquerda, coluna do dataframe da direita, o parametro how do merge, e o nome do dataframe de destino. Nunca utilize nomes de dataframes que não existam. Exemplo: MergeDataframesAgent(dataframe1='df1', dataframe2='df2', left_on='coluna_df1', right_on='coluna_df2', how='left', destination='df_final')",
            agent_prompt=(
                """
                    Agente que da merge dataframes. 
                    Voce possui uma tool que recebe o nome dos dataframes a serem unidos, a coluna do dataframe da esquerda, coluna do dataframe da direita, o parametro how do merge, e o nome do dataframe de destino.
                """
            )
        )

        response = await agent.tool_agent(query)

        return response

    def export_df(self, dataframe_name):
        return self.pandas_executor.export_df(dataframe_name)
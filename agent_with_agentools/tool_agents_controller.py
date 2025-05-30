from agent_with_agentools.tool_agent import ToolAgent
from simple_agent.exec_func import PandasExecutor

class ToolAgentsController:
    def __init__(self, dataframes):
        self.pandas_executor = PandasExecutor(dataframes)
        self.tokens_in = 0
        self.tokens_out = 0

    def get_infos(self):
        return self.pandas_executor.get_infos()
    
    async def generate_unique_id(self, dataframes: list, nome_coluna_nome: list, nome_coluna_documento: list):
        agent = ToolAgent(
            self.pandas_executor.gera_id_unico,
            tool_name="GenerateUniqueID",
            tool_description="Funcao que gera um id unico para cada colaborador. Voce deve passar os dataframes, os nomes das colunas de nome e documento.",
            agent_prompt=(
                """
                    Agente que gera um id unico para cada colaborador em todas as planilhas que voce esta manipulando.
                    Voce possui uma tool que recebe a lista dos dataframes, os nomes das colunas de nome e documento.
                    Responda em apenas uma linha exatamente a operação que você realizou, com todos os parametros. Explique objetivamente.
                """
            )
        )

        query = f"dataframes={dataframes}, nome_coluna_nome={nome_coluna_nome}, nome_coluna_documento={nome_coluna_documento}"

        (response, token_usage) = await agent.tool_agent(query)

        self.tokens_in += token_usage['prompt_tokens']
        self.tokens_out += token_usage['completion_tokens']

        return response

    async def rename_column_agent(self, dataframe_to_change: str, columns: list, new_columns: list):
        agent = ToolAgent(
            self.pandas_executor.rename_columns,
            tool_name="RenameColumnsAgent",
            tool_description="Agente capaz de renomear as colunas de um dataframe. Voce deve passar o nome do dataframe, as colunas a serem renomeadas e os novos nomes.",
            agent_prompt=(
                """
                    Agente que renomeia as colunas de um dataframe. 
                    Voce possui uma tool aque recebe o nome do dataframe, as colunas a serem renomeadas e os novos nomes.
                    Responda em apenas uma linha exatamente a operação que você realizou, com todos os parametros. Explique objetivamente.
                """
            )
        )

        query = f"dataframe_to_change={dataframe_to_change}, columns={columns}, new_columns={new_columns}"

        (response, token_usage) = await agent.tool_agent(query)

        self.tokens_in += token_usage['prompt_tokens']
        self.tokens_out += token_usage['completion_tokens']

        return response
    
    async def remove_columns_agent(self, dataframe_to_change: str, columns: list):
        agent = ToolAgent(
            self.pandas_executor.remove_columns,
            tool_name="RemoveColumnsAgent",
            tool_description="Agente capaz de remover colunas de um dataframe. Voce deve passar o nome do dataframe e a lista de colunas a serem removidas.",
            agent_prompt=(
                """
                    Agente que remove colunas de um dataframe. 
                    Voce possui uma tool que recebe o nome do dataframe e a lista de colunas a serem removidas.
                    Responda em apenas uma linha exatamente a operação que você realizou, com todos os parametros. Explique objetivamente.
                """
            )
        )

        query = f"dataframe_to_change={dataframe_to_change}, columns={columns}"

        (response, token_usage) = await agent.tool_agent(query)

        self.tokens_in += token_usage['prompt_tokens']
        self.tokens_out += token_usage['completion_tokens']

        return response
    
    async def select_columns_agent(self, dataframe: str, columns: list):
        agent = ToolAgent(
            self.pandas_executor.select_columns,
            tool_name="SelectColumnsAgent",
            tool_description="Agente capaz de selecionar colunas de um dataframe. Voce deve passar o nome do dataframe e as colunas a serem selecionadas.",
            agent_prompt=(
                """
                    Agente que seleciona colunas de um dataframe. 
                    Voce possui uma tool que recebe o nome do dataframe e as colunas a serem selecionadas.
                    Responda em apenas uma linha exatamente a operação que você realizou, com todos os parametros. Explique objetivamente.
                """
            )
        )
        
        query = f"dataframe={dataframe}, columns={columns}"

        respose = await agent.tool_agent(query)
        return respose
    
    async def sum_columns_agent(self, dataframe: str, columns: list, new_column_name: str):
        agent = ToolAgent(
            self.pandas_executor.soma_colunas,
            tool_name="SumColumnsAgent",
            tool_description="Agente capaz de somar colunas de um dataframe. Voce deve passar o nome do dataframe, as colunas a serem somadas e o novo nome da coluna.",
            agent_prompt=(
                """
                    Agente que soma colunas de um dataframe. 
                    Voce possui uma tool que recebe o nome do dataframe, as colunas a serem somadas e o novo nome da coluna.
                    Responda em apenas uma linha exatamente a operação que você realizou, com todos os parametros. Explique objetivamente.
                """
            )
        )

        query = f"dataframe={dataframe}, columns={columns}, new_column_name={new_column_name}"

        (response, token_usage) = await agent.tool_agent(query)

        self.tokens_in += token_usage['prompt_tokens']
        self.tokens_out += token_usage['completion_tokens']

        return response

    async def merge_dataframes_agent(self, dataframe1: str, dataframe2: str, left_on: str, right_on: str, how: str, destination: str):
        agent = ToolAgent(
            self.pandas_executor.merge_dataframes,
            tool_name="MergeDataframesAgent",
            tool_description="Função que faz o merge de dois dataframes. Deve ser passado o nome dos dataframes a serem unidos, a coluna do dataframe da esquerda, coluna do dataframe da direita, o parametro how do merge, e o nome do dataframe de destino. Nunca utilize nomes de dataframes que não existam. Exemplo: MergeDataframesAgent(dataframe1='df1', dataframe2='df2', left_on='coluna_df1', right_on='coluna_df2', how='left', destination='df_final')",
            agent_prompt=(
                """
                    Agente que da merge dataframes. 
                    Voce possui uma tool que recebe o nome dos dataframes a serem unidos, a coluna do dataframe da esquerda, coluna do dataframe da direita, o parametro how do merge, e o nome do dataframe de destino.
                    Responda em apenas uma linha exatamente a operação que você realizou, com todos os parametros. Explique objetivamente.
                """
            )
        )

        query = f"dataframe1={dataframe1}, dataframe2={dataframe2}, left_on={left_on}, right_on={right_on}, how={how}, destination={destination}"

        (response, token_usage) = await agent.tool_agent(query)

        self.tokens_in += token_usage['prompt_tokens']
        self.tokens_out += token_usage['completion_tokens']

        return response

    def export_df(self, dataframe_name):
        return self.pandas_executor.export_df(dataframe_name)
    
    def print_token_count(self):
        print(f"Tokens In: {self.tokens_in}, Tokens Out: {self.tokens_out}")
        return {
            'tokens_in': self.tokens_in,
            'tokens_out': self.tokens_out
        }
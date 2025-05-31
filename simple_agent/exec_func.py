import pandas as pd
import hashlib
import uuid

from typing import TypedDict, List, Dict

class PandasExecutor:
    def __init__(self, dataframes):
        
        self.dataframes = dataframes
                
    def get_infos(self):
        """
            Funcao que retorna as informacoes dos dataframes
        """

        def format_df_info(df, name):
            object_cols_with_duplicates = []
            object_columns = df.select_dtypes(include=['object']).columns
            
            for col in object_columns:
                if df[col].duplicated().any():
                    object_cols_with_duplicates.append(
                        f"Coluna: {col} com valores ducplicados"
                    )
            
            duplicates_info = "\n            ".join(object_cols_with_duplicates) if object_cols_with_duplicates else "Nenhuma"
            
            return f"""
            {name}:
            Shape: {df.shape}
            Columns: {list(df.columns)}
            Data types: 
            {df.dtypes}
            Colunas object com valores duplicados:
                {duplicates_info}
            """
        
        all_info = []    

        for dataframe in self.dataframes:
            all_info.append(format_df_info(self.dataframes[dataframe], dataframe))

        return "\n\n".join(all_info)
    
    def gera_id_unico(self, dataframes: list, nome_coluna_nome: list, nome_coluna_documento: list):

        """
            Funcao que gera um id unico para cada colaborador
        """

        for i, dataframe in enumerate(dataframes):
            col_nome = nome_coluna_nome[i]
            col_doc = nome_coluna_documento[i]
            
            def generate_uuid_for_row(row):
                if pd.isna(row[col_nome]) or pd.isna(row[col_doc]) or \
                    str(row[col_nome]).strip() == '' or str(row[col_doc]).strip() == '':
                    return None
                
                colaborador_string = f"{row[col_nome]}_{row[col_doc]}"
                
                namespace = uuid.NAMESPACE_DNS
                return str(uuid.uuid5(namespace, colaborador_string))
            
            self.dataframes[dataframe]["id_unico"] = self.dataframes[dataframe].apply(
                generate_uuid_for_row, axis=1
            )

        return self.get_infos()
        
        
    def rename_columns(self, dataframe_to_change: str, columns: list, new_columns: list):
        """
            Funcao que renomeia as colunas de um dataframe
        """

        try:
            
            new_dataframe: pd.DataFrame = self.dataframes[dataframe_to_change].copy()
            
            
            new_dataframe.rename(columns=dict(zip(columns, new_columns)), inplace=True)
            
            self.dataframes[dataframe_to_change] = new_dataframe
            
            return f"Colunas renomeadas com sucesso!\n\n"
        except Exception as e:
            return f"Erro ao renomear colunas: {e}" 
    
    class DataframeInfo(TypedDict):
        previous_names: List[str]
        new_names: List[str]
    
    Dataframe = Dict[str, DataframeInfo]

    def rename_multiple_dataframe_columns(self, dataframes: Dataframe):
        """
            Funcao que renomeia colunas de varios dataframes de uma vez
            Parâmetros:
                dataframes (dict): Dicionário onde as chaves são os nomes dos dataframes e os valores são dicionários com as colunas anteriores e novas
                    Exemplo:
                    {
                        'dataframe1': {'previous_names': ['col1', 'col2'], 'new_names': ['nova_col1', 'nova_col2']},
                        'dataframe2': {'previous_names': ['col3'], 'new_names': ['nova_col3']}
                    } 
        """

        try:
            for dataframe_name, info in dataframes.items():
                df: pd.DataFrame = self.dataframes[dataframe_name].copy()

                df.rename(columns=dict(zip(info['previous_names'], info['new_names'])), inplace=True)

                self.dataframes[dataframe_name] = df
            return "Colunas renomeadas com sucesso!\n\n"
        except Exception as e:
            return f"Erro ao renomear colunas: {e}"

    def remove_columns(self, dataframe_to_change: str, columns: list):
        """
            Funcao que remove as colunas de um dataframe
        """
        try:
            new_dataframe: pd.DataFrame = self.dataframes[dataframe_to_change].copy()
            
            new_dataframe.drop(columns=columns, inplace=True)
            
            self.dataframes[dataframe_to_change] = new_dataframe
            
            return "Colunas removidas com sucesso!\n\n"
        except Exception as e:
            return f"Erro ao remover colunas: {e}"
        
    def soma_colunas(self, dataframe: str, columns: list, new_column_name: str):
        """
            Funcao que soma as colunas de um dataframe
        """
        try:
            df = self.dataframes[dataframe].copy()
            
            df[new_column_name] = df[columns].sum(axis=1)
            
            self.dataframes[dataframe] = df
            return "Colunas somadas com sucesso!\n\n"
        except Exception as e:
            return f"Erro ao somar colunas: {e}"        
                
    def select_columns(self, dataframe: str, columns: list):
        """
            Função que modifica um dataframe para conter apenas as colunas especificadas
            Parâmetros:
                dataframe (str): Nome do dataframe a ser modificado
                columns (list): Lista com os nomes das colunas a serem mantidas
            
            Retorno:
                str: Mensagem de sucesso ou erro
        """
        
        try:
            df = self.dataframes[dataframe].copy()
            
            for col in columns:
                if col not in df.columns:
                    return f"Erro: A coluna '{col}' não existe no dataframe '{dataframe}'."
            
            df = df[columns]
            self.dataframes[dataframe] = df
            
            return "Colunas selecionadas com sucesso!\n\n"
        except Exception as e:
            return f"Erro ao selecionar colunas: {e}"

    def select_multiple_df_columns(self, dataframes: dict):
        """
            Funcao que modifica múltiplos dataframes para conter apenas as colunas especificadas
            Parametros:
                dataframes (dict): Dicionário onde as chaves são os nomes dos dataframes e os valores são listas de colunas a serem mantidas

            Exemplo: { 'dataframe1': ['col1', 'col2'], 'dataframe2': ['col3', 'col4'] }
        """

        try:
            for dataframe, columns in dataframes.items():
                df = self.dataframes[dataframe].copy()

                
                df = df[columns]
                self.dataframes[dataframe] = df
            
            return "Colunas selecionadas com sucesso!\n\n"
        except Exception as e:
            return f"Erro ao selecionar colunas: {e}"

    def sum_column_groups(self, dataframe: str, groups: dict):
        """
            Função que soma múltiplos grupos de colunas de um dataframe
            
            Parâmetros:
            dataframe (str): Nome do dataframe onde as somas serão realizadas
            groups (dict): Dicionário onde as chaves são os nomes das novas colunas e os valores são listas de colunas a serem somadas
                        Exemplo: {'Custo_Ferramentas': ['ferramenta1', 'ferramenta2'], 'Custo_Beneficios': ['beneficio1', 'beneficio2']}
            
            Retorno:
            str: Mensagem de sucesso
        """

        try:
            df = self.dataframes[dataframe].copy()
            
            somas_realizadas = []
            
            for new_column_name, columns_to_sum in groups.items():
                for col in columns_to_sum:
                    if col not in df.columns:
                        return f"Erro: A coluna '{col}' não existe no dataframe '{dataframe}'."
                
                if len(columns_to_sum) <= 1:
                    return f"Erro: É necessário fornecer mais de uma coluna para a soma. {columns_to_sum}"
                
                df[new_column_name] = df[columns_to_sum].sum(axis=1)
                somas_realizadas.append(f"'{new_column_name}': {columns_to_sum}")
            
            self.dataframes[dataframe] = df
            
            return "Colunas somadas com sucesso!\n\n"
        except Exception as e:
            return f"Erro ao somar colunas: {e}"

    def merge_dataframes(self, dataframe1: str, dataframe2: str, left_on: str, right_on: str, how: str, destination: str):
        """
            Funcao que faz o merge de dois dataframes
        """
        try :
            df1 = self.dataframes[dataframe1].copy()
            df2 = self.dataframes[dataframe2].copy()

            new_dataframe = pd.merge(df1, df2, left_on=left_on, right_on=right_on, how=how)

            self.dataframes[destination] = new_dataframe

            return "Dataframes unidos com sucesso!\n\n"
        except Exception as e:
            return f"Erro ao unir dataframes: {e}"
        
    def merge_multiple_dataframes(self, dataframes_list: list, on_column: str, how: str, destination: str):
        """
            Função que faz o merge de múltiplos dataframes usando uma coluna em comum
            
            Parâmetros:
            dataframes_list (list): Lista com os nomes dos dataframes a serem unidos
            on_column (str): Nome da coluna a ser usada como base para o merge (geralmente "id_unico")
            how (str): Tipo de merge a ser realizado ('left', 'right', 'inner', 'outer')
            destination (str): Nome do dataframe de destino onde o resultado será armazenado
            
            Retorno:
            str: Mensagem de sucesso
        """
        try:
            result_df = self.dataframes[dataframes_list[0]].copy()
            
            for i in range(1, len(dataframes_list)):
                df_name = dataframes_list[i]
                next_df = self.dataframes[df_name].copy()
                
                result_df = pd.merge(
                    result_df, 
                    next_df, 
                    left_on=on_column, 
                    right_on=on_column, 
                    how=how
                )
            
            self.dataframes[destination] = result_df
            
            return "Dataframes unidos com sucesso!\n\n"
        except Exception as e:
            return f"Erro ao unir dataframes: {e}"

    def export_df(self, dataframe_to_export: str):
        """
            Funcao capaz de exportar o df_final
        """
        try:
            self.dataframes[dataframe_to_export].to_excel("output/custos_por_colaborador.xlsx")
            
            return "Dataframe exportado com sucesso!\n\n"
        except Exception as e:
            return f"Erro ao exportar o dataframe {e}\n\n"

    def generate_html_analysis(self, final_dataframe: str):
        try:
            df_final: pd.DataFrame = self.dataframes[final_dataframe]

            numerical_columns = df_final.select_dtypes(include="number")

            df_mean = numerical_columns.mean()

            data = {}

            for column in df_mean.keys():
                data[column] = df_mean[column]

            return data
        except Exception as e:
            return f"Erro ao gerar dados HTML {e}\n\n"

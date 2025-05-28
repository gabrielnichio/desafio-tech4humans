import pandas as pd

class PandasExecutor:
    def __init__(self, dataframes):
        
        self.dataframes = dataframes
                
    def get_infos(self):
        """
            Funcao que retorna as informacoes dos dataframes
        """

        def format_df_info(df, name):
            return f"""
            {name}:
            Shape: {df.shape}
            Columns: {list(df.columns)}
            Data types: 
            {df.dtypes}
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
            
            self.dataframes[dataframe]["id_unico"] = self.dataframes[dataframe].apply(
                lambda x: f"{x[col_nome]}_{x[col_doc]}", axis=1
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
        
        df = self.dataframes[dataframe].copy()
        
        df[new_column_name] = df[columns].sum(axis=1)
        
        self.dataframes[dataframe] = df
                
        
        print(f"Colunas {columns} somadas com sucesso e armazenadas na coluna {new_column_name}!\n\n")

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
        
        df1 = self.dataframes[dataframe1].copy()
        df2 = self.dataframes[dataframe2].copy()
        
        new_dataframe = pd.merge(df1, df2, left_on=left_on, right_on=right_on, how=how)
        
        self.dataframes[destination] = new_dataframe
        
        print(f"Dataframes {dataframe1} e {dataframe2} unidos com sucesso!\n\n")

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
            self.dataframes[dataframe_to_export].to_excel("teste.xlsx")
            
            return "Dataframe exportado com sucesso!\n\n"
        except Exception as e:
            return f"Erro ao exportar o dataframe {e}\n\n"

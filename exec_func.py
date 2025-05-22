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
        new_dataframe: pd.DataFrame = self.dataframes[dataframe_to_change].copy()
        
        
        new_dataframe.rename(columns=dict(zip(columns, new_columns)), inplace=True)
        
        self.dataframes[dataframe_to_change] = new_dataframe
        
        print(f"Colunas {columns} renomeadas para {new_columns} com sucesso!\n\n")
        
        
        
    def remove_columns(self, dataframe_to_change: str, columns: list):
        """
            Funcao que remove as colunas de um dataframe
        """
        new_dataframe: pd.DataFrame = self.dataframes[dataframe_to_change].copy()
        
        new_dataframe.drop(columns=columns, inplace=True)
        
        self.dataframes[dataframe_to_change] = new_dataframe
        
        print(f"Colunas {columns} removidas com sucesso!\n\n")
        
        
        
        
    def criar_dataframe(self, dataframe_name: str, columns: list):
        """
            Funcao que cria um novo dataframe
        """
        new_dataframe = pd.DataFrame(columns=columns)
                
        self.dataframes[dataframe_name] = new_dataframe
        
        print(f"Dataframe {dataframe_name} criado com sucesso!\n\n")
        
        
        
    def soma_colunas(self, dataframe: str, columns: list, new_column_name: str):
        """
            Funcao que soma as colunas de um dataframe
        """
        
        df = self.dataframes[dataframe].copy()
        
        df[new_column_name] = df[columns].sum(axis=1)
        
        self.dataframes[dataframe] = df
                
        
        print(f"Colunas {columns} somadas com sucesso e armazenadas na coluna {new_column_name}!\n\n")

    def merge_dataframes(self, dataframe1: str, dataframe2: str, left_on: str, right_on: str, how: str, destination: str):
        """
            Funcao que faz o merge de dois dataframes
        """
        
        df1 = self.dataframes[dataframe1].copy()
        df2 = self.dataframes[dataframe2].copy()
        
        new_dataframe = pd.merge(df1, df2, left_on=left_on, right_on=right_on, how=how)
        
        self.dataframes[destination] = new_dataframe
        
        print(f"Dataframes {dataframe1} e {dataframe2} unidos com sucesso!\n\n")
        

    def export_df(self, dataframe_to_export: str):
        """
            Funcao capaz de exportar o df_final
        """
        self.dataframes[dataframe_to_export][["ID", "Nome", "Centro de Custo", "Custo por Ferramenta", "Custo por Beneficio", "Custo Total"]].to_excel("teste.xlsx")
        
        print("Dataframe exportado com sucesso!\n\n")


# gere o dataframe final calculando os custos para cada colaborador. Manipule e padronize os dados das diferentes planilhas para conseguir fazer isso corretamente
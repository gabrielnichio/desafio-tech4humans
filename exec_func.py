import pandas as pd

from llama_index.llms.anthropic import Anthropic

class PandasExecutor:
    def __init__(self, colab, unimed, gympass, github, google):
        self.llm = Anthropic(model="claude-3-5-haiku-latest", temperature=0, timeout=None, max_retries=2)
        
        self.colab = colab
        self.unimed = unimed
        self.gympass = gympass
        self.github = github
        self.google = google
        self.df_final = pd.DataFrame({"ID": pd.Series(dtype='str'), "Nome": pd.Series(dtype='str'), "Centro de Custo": pd.Series(dtype='str'), "Custo por Ferramenta": pd.Series(dtype=float), "Custo por Beneficio": pd.Series(dtype=float), "Custo Total": pd.Series(dtype=float)})
        
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
            
            First 5 rows:
            {df.head().to_string()}
            """
        
        colab_info = format_df_info(self.colab, "colab")
        unimed_info = format_df_info(self.unimed, "unimed")
        gympass_info = format_df_info(self.gympass, "gympass")
        github_info = format_df_info(self.github, "github")
        google_info = format_df_info(self.google, "google")
        df_final_info = format_df_info(self.df_final, "df_final")
        
        return colab_info + unimed_info + gympass_info + github_info + google_info + df_final_info
    
    def execute(self, codigo):
        """
            Função que capaz de executar um codigo python. As variaveis locais devem ser executadas e um df_final deve ser atribuido. Apenas a biblioteca pandas(pd) pode ser utilizada.
            
            local_vars = {
                "colab": self.colab,
                "unimed": self.unimed,
                "gympass": self.gympass,
                "github": self.github,
                "google": self.google,
                "df_final": self.df_final
            }
            
        """

        local_vars = {
            "colab": self.colab,
            "unimed": self.unimed,
            "gympass": self.gympass,
            "github": self.github,
            "google": self.google,
            "df_final": self.df_final
        }
                    
        exec(codigo, globals(), local_vars)
        
        self.df_final = local_vars["df_final"]

        print("\n\n--------------\n\nDF_FINAL: ", self.df_final.head(), "\n\n")

    def export_df(self):
        """
            Funcao capaz de exportar o df_final
        """
        self.df_final.to_excel("teste.xlsx")
        
        print("Dataframe exportado com sucesso!")


# gere o dataframe final calculando os custos para cada colaborador. Manipule e padronize os dados das diferentes planilhas para conseguir fazer isso corretamente
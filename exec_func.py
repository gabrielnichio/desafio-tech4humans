import pandas as pd

class PandasExecutor:
    def __init__(self, colab, google):
        self.colab = colab
        self.google = google
    
    def execute(self, comando):
        local_vars = {
            "colab": self.colab,
            "google": self.google,
            "df_final": pd.DataFrame({"A": pd.Series(dtype='float')})
        }
        
        exec(comando, globals(), local_vars)
        
        self.df = local_vars["df_final"]
        
        return self.df

# Uso
colab = pd.read_excel("data/Dados Colaboradores.xlsx")
google = pd.read_excel("data/Ferramentas/Ferramenta 2 - Google workspace.xlsx")
executor = PandasExecutor(colab, google)
executor.execute("df_final['A'] = colab['Salario'].reset_index(drop=True) + google['Valor Mensal'].reset_index(drop=True)")
executor.df.to_excel("teste.xlsx", index=False)
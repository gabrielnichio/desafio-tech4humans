import pandas as pd

class PandasExecutor:
    def __init__(self, colab, google):
        self.colab = colab
        self.google = google
        self.df_final: pd.DataFrame
    
    def execute(self, comando):
        """
            Funcao que executa apenas uma linha de codigo pandas. Eh possivel realizar operacoes utilizando o dataframe de colaboradores 'colab', o dataframe
            da ferramenta Google Workspace 'google' e o 'df_final' que eh um: pd.DataFrame({"A": pd.Series(dtype='float')}).

            A funcao recebe:
                comando: string contendo uma linha de codigo

            O codigo deve ser capaz de ser executado dentro da funcao exec() do Python. Apenas a biblioteca pandas as pd esta disponivel para manipulacoes.

            Essas sao as local_vars:

            local_vars = {
                "colab": self.colab,
                "google": self.google,
                "df_final": pd.DataFrame({"A": pd.Series(dtype='float')})
            }
        """

        print("Comando mandado: ", str(comando))

        local_vars = {
            "colab": self.colab,
            "google": self.google,
            "df_final": pd.DataFrame({"A": pd.Series(dtype='float')})
        }
        
        exec(comando, globals(), local_vars)

        # print("Local vars: ", local_vars)
        
        self.df_final = local_vars["df_final"]

        print("DF_FINAL: ", self.df_final.head())

    def export_df(self):
        """
            Funcao capaz de exportar o df_final
        """
        self.df_final.to_excel("teste.xlsx")

# # Uso
# colab = pd.read_excel("data/Dados Colaboradores.xlsx")
# google = pd.read_excel("data/Ferramentas/Ferramenta 2 - Google workspace.xlsx")
# executor = PandasExecutor(colab, google)
# executor.execute("df_final['A'] = colab['Salario'].reset_index(drop=True) + google['Valor Mensal'].reset_index(drop=True)")
# executor.df.to_excel("teste.xlsx", index=False)
# some a coluna Salario do dataframe colab e a coluna Valor Mensal do dataframe google, armazene na coluna A do dataframe final e exporte esse dataframe
import pandas as pd

from llama_index.llms.anthropic import Anthropic

class PandasExecutor:
    def __init__(self, dataframes):
        self.llm = Anthropic(model="claude-3-5-sonnet-latest", max_tokens = 2000, temperature=0, timeout=None, max_retries=2)
        
        self.dataframes = dataframes
        
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
        all_info = []    
        
        for dataframe in self.dataframes:
            all_info.append(format_df_info(self.dataframes[dataframe], dataframe))
                
        return "\n\n".join(all_info)
    
    def execute(self, codigo):        
        f"""
            Função que capaz de executar um codigo python. As variaveis locais devem ser executadas e um df_final deve ser atribuido. Apenas a biblioteca pandas(pd) pode ser utilizada.            
        """
        
        response = self.llm.complete(
            prompt=f"""
                Forneça APENAS código Python sem explicações adicionais.
                O código deve ser conciso, funcional e pronto para execução.
                Não inclua blocos de markdown, símbolos de cópia, linhas de comentário iniciais como ```python ou ``` no final.
                Seu código será executado diretamente com exec() em Python.
                Você deve usar a biblioteca pandas (pd) para manipular os dataframes. 
                
                O seu código deve ser capaz de manipular os dataframes:
                
                {self.get_infos()}
                
                e construir um dataframe final (df_final) com as seguintes colunas:
                ID, Nome, Centro de Custo, Custo por Ferramenta, Custo por Beneficio e Custo Total.
                
                Você está trabalhando em um ambiente local. As variaveis dos dataframes que você usar no código serão as variaveis locais.
            """
        )
        
        code = response.text.strip()
    
        # Remover blocos de código markdown se existirem
        if code.startswith("```python"):
            code = code.replace("```python", "", 1)
        if code.endswith("```"):
            code = code[:-3]
            
        print("\n\n--------------\n\nCodigo: ", code.strip(), "\n\n")

        local_vars = {}
        
        for dataframe in self.dataframes:
            local_vars[dataframe] = self.dataframes[dataframe]
        
        local_vars["df_final"] = self.df_final
                    
        exec(code.strip(), globals(), local_vars)
        
        self.df_final = local_vars["df_final"]

        print("\n\n--------------\n\nDF_FINAL: ", self.df_final.head(), "\n\n")

    def export_df(self):
        """
            Funcao capaz de exportar o df_final
        """
        self.df_final.to_excel("teste.xlsx")
        
        print("Dataframe exportado com sucesso!")


# gere o dataframe final calculando os custos para cada colaborador. Manipule e padronize os dados das diferentes planilhas para conseguir fazer isso corretamente
import pandas as pd
from dotenv import load_dotenv
load_dotenv()

from llama_index.llms.anthropic import Anthropic
from llama_index.llms.openai import OpenAI

from llama_index.experimental.query_engine.pandas import PandasQueryEngine

from llama_index.core import Settings

class QueryEngine:
    def __init__(self, data_path: str):
        llm = Anthropic(model="claude-3-5-haiku-latest", temperature=0, timeout=None, max_retries=2)
        # llm = OpenAI(model="o3-mini", temperature=0.5, max_retries=2)
        Settings.llm = llm
        
        self.df = pd.read_excel(data_path)
        self.qe = None
        self._init_query_engine()
        
    def _init_query_engine(self):
        f"""Query engine que, a partir de um texto, gera comandos pandas no python."""
        
        self.qe = PandasQueryEngine(df=self.df, verbose=True)
        
        instruction_str = """\
            1. Convert the query to executable Python code using Pandas.
            2. The final line of code should be a Python expression that can be called with the `eval()` function.
            3. The code should represent a solution to the query.
            4. PRINT ONLY THE EXPRESSION.
            5. Do not quote the expression.
            6. Always use the pandas.set_option('display.max_rows', None) to always show all the rows
            """
            
        self.qe.update_prompts({"instruction_str": instruction_str})
        
    def get_QE(self):
        return self.qe
        
    
    
        
    
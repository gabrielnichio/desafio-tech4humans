import asyncio
# uncomment to use Agent with agents as tools
# from agent_with_agentools.agent_with_agent import Agent 

# uncomment to use Single Agent with tools
from simple_agent.agent import Agent

from fastapi import FastAPI, UploadFile
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

import uvicorn

class Item(BaseModel):
    aaa: str
    infos: str
    
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

agent = Agent()

@app.post("/")
def init(item: Item):
    print(item)
    if agent:
        return {"message": "Agent is running"}
    
@app.post("/uploadfiles/")
async def create_upload_files(files: list[UploadFile]):
    agent.set_dataframes(files)

    return {"message": "Dataframes carregados com sucesso."}


@app.get("/agent")
def run_agent():

    if not agent.verify_dataframes():
        return {"message": "Please upload more than one dataframe."}
    else:
        asyncio.run(agent.run("""
            utilize suas ferramentas para analisar os dataframes existentes, maniula-los, construa e exporte um dataframe que contenha os custos por colaborador APENAS com colunas: 
                -ID: identificador do colaborador (id unico)
                -Nome: Nome do colaborador
                -Centro de Custo: centro de custo do colaborador
                -uma coluna para cada custo, sendo o nome da coluna o nome do custo (incluindo salario, se houver)
                -Custo Total: soma de todos os custos do colaborador
                
            Não insira mais nenhuma coluna relacionada a outras coisas no dataframe final.
        """))

        return FileResponse(
            path="output/custos_por_colaborador.xlsx",
            filename="custos_por_colaborador.xlsx",
            media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
    
    
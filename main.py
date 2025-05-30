import asyncio
from agent_with_agentools.agent_with_agent import Agent 
from simple_agent.agent import Agent

if __name__ == "__main__":
    agent = Agent()
    
    asyncio.run(agent.run("""
        gere e exporte um dataframe que calcula os custos por colaborador e exporte-o APENAS com colunas: 
            -ID: identificador do colaborador (id unico)
            -Nome: Nome do colaborador
            -Centro de Custo: centro de custo do colaborador
            -uma coluna para cada custo, sendo o nome da coluna o nome do custo (incluindo salario)
            -Custo Total: soma de todos os custos do colaborador
            
        Não insira mais nenhuma coluna relacionada a outras coisas no dataframe final.
    """))
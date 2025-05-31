# Agente manipulador de dados

Projeto desenvolvido para o desafio Techlab proposto pela Tech4Humans. O projeto consiste em um agente que seja capaz de manipular planilhas de diversos custos de colaboradores de uma empresa.

## Ferramentas utilizadas

- Pandas
- LlamaIndex para desenvolvimentos dos agentes
- APIs da OpenAI e Anthropic para obtenção das LLMs
- Backend em Fastapi
- Frontend em HTML, CSS e Javascript

## Features
 
- Agente simples: foi implementado uma versão com apenas um único agente que possui diversas ferramentas para manipular os dados
- Agente com agentolls: essa versão possui um agente "mãe" que utiliza outros agentes como ferramentas. Cada agente é responsável por executar uma função de manipulação.

## Fluxo

1. O usuário faz o upload das planilhas que deseja manipular no frontend
2. Backend receberá as planilhas e irá armazena-las no escopo
3. O agent irá maniuplar os dados utilizando ferramentas que possuem códigos Pandas
4. O agent montará o dataframe final de acordo com as especificações do prompt
5. O backend retornará o arquivo excel final para o frontend e o download será feito automaticamente

## Rodar o projeto

1. Instalar requirements
```
pip install -r requirements.txt
```

2. Rodar o backend fastapi com uvicorn
```
uvicorn main:app
```

3. Abrir o html que está na pasta frontend
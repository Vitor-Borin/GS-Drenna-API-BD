from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import oracledb
import os
from dotenv import load_dotenv
load_dotenv()

app = FastAPI()

# Configuração CORS
origins = [
    "http://localhost:8000",
    "http://127.0.0.1:8000",
    "http://127.0.0.1:5500",  # << Adicione essa linha
    "http://localhost:5500",   # E se quiser incluir localhost também
]


app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,  # libera esses domínios
    allow_credentials=True,
    allow_methods=["*"],  # libera todos os métodos (GET, POST, etc)
    allow_headers=["*"],  # libera todos os headers
)

# Conexão com o banco Oracle
conn = oracledb.connect(
    user=os.getenv("ORACLE_USER"),
    password=os.getenv("ORACLE_PASSWORD"),
    dsn=oracledb.makedsn(
        os.getenv("ORACLE_HOST"),
        int(os.getenv("ORACLE_PORT")),
        sid=os.getenv("ORACLE_SID")
    )
)
# Lista de tabelas que o sistema pode acessar
tabelas_permitidas = ["GS_CIDADE", "GS_BAIRRO", "GS_PREVISAO_TEMPO", "GS_FAMILIARES_DESAPARECIDOS", "GS_NIVEL_ALERTA"]

@app.get("/tabela/{nome_tabela}")
async def listar_tabela(nome_tabela: str):
    nome_tabela = nome_tabela.upper()

    if nome_tabela not in tabelas_permitidas:
        raise HTTPException(status_code=403, detail="Tabela não permitida")

    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {nome_tabela}")
        colunas = [col[0] for col in cursor.description]

        dados = []
        for row in cursor.fetchall():
            linha = {}
            for col, val in zip(colunas, row):
                if hasattr(val, "read"):  # se for CLOB/BLOB
                    val = val.read()      # converte para string
                linha[col] = val
            dados.append(linha)

        return dados

    except Exception as e:
        print("Erro:", e)
        raise HTTPException(status_code=400, detail=str(e))


from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import oracledb
import os
from dotenv import load_dotenv
import asyncio

load_dotenv()

app = FastAPI()

origins = [
    "*"
]


app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

db_pool = None

@app.on_event("startup")
async def startup_db_pool():
    global db_pool
    try:
        db_pool = await oracledb.create_pool(
            user=os.getenv("ORACLE_USER"),
            password=os.getenv("ORACLE_PASSWORD"),
            dsn=oracledb.makedsn(
                os.getenv("ORACLE_HOST"),
                int(os.getenv("ORACLE_PORT")),
                sid=os.getenv("ORACLE_SID")
            ),
            min=1,
            max=5
        )
        print("Pool de conexão Oracle criado com sucesso!")
    except Exception as e:
        print("Erro ao criar pool de conexão Oracle", e)

@app.on_event("shutdown")
async def shutdown_db_pool():
    global db_pool
    if db_pool:
        await db_pool.close()
        print("Pool de conexão Oracle fechado.")


tabelas_permitidas = ["GS_CIDADE", "GS_BAIRRO", "GS_PREVISAO_TEMPO", "GS_FAMILIARES_DESAPARECIDOS", "GS_NIVEL_ALERTA"]

@app.get("/tabela/{nome_tabela}")
async def listar_tabela(nome_tabela: str):
    nome_tabela = nome_tabela.upper()

    if nome_tabela not in tabelas_permitidas:
        raise HTTPException(status_code=403, detail="Tabela não permitida")

    async with db_pool.acquire() as connection:
        async with connection.cursor() as cursor:
            try:
                await cursor.execute(f"SELECT * FROM {nome_tabela}")
                colunas = [col[0] for col in cursor.description]

                dados = []
                for row in await cursor.fetchall():
                    linha = {}
                    for col, val in zip(colunas, row):
                        if val is not None and hasattr(val, "read"):
                            val = await val.read()
                            if isinstance(val, bytes):
                                try:
                                    val = val.decode('utf-8')
                                except UnicodeDecodeError:
                                     pass

                        linha[col] = val
                    dados.append(linha)

                return dados

            except Exception as e:
                print(f"Erro ao listar tabela {nome_tabela}:", e)
                raise HTTPException(status_code=400, detail=f"Erro ao acessar dados da tabela {nome_tabela}: {e}")


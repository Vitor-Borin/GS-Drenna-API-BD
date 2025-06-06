from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
import oracledb
import os
from dotenv import load_dotenv
import asyncio

load_dotenv()

app = FastAPI()

origins = [
    "*" # Permitir todas as origens por enquanto, ajustar conforme necessário
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"], # Permitir todos os métodos, ajustar conforme necessário
    allow_headers=["*"], # Permitir todos os headers, ajustar conforme necessário
)

# Declarar db_pool como None globalmente
db_pool = None

@app.on_event("startup")
async def startup_db_pool():
    """Cria a pool de conexão Oracle ao iniciar a aplicação."""
    global db_pool
    try:
        # Usar create_pool_async para pool assíncrona
        db_pool = await oracledb.create_pool_async(
            user=os.getenv("ORACLE_USER"),
            password=os.getenv("ORACLE_PASSWORD"),
            dsn=oracledb.makedsn(
                os.getenv("ORACLE_HOST"),
                int(os.getenv("ORACLE_PORT")), # Converter porta para inteiro
                sid=os.getenv("ORACLE_SID")
            ),
            min=1,
            max=5,
            # Adicionar timeouts pode ajudar a identificar problemas de conexão
            # expire_time=60, # Tempo em segundos para uma conexão inativa ser fechada
            # getmode=oracledb.POOL_GETMODE_TIMEDWAIT, # Esperar por uma conexão se a pool estiver cheia
            # wait_timeout=5 # Tempo máximo de espera em segundos
        )
        print("Pool de conexão Oracle criada com sucesso!")
    except Exception as e:
        # Imprimir a exceção diretamente para depuração
        print(f"Erro ao criar pool de conexão Oracle: {e}")
        # Em um ambiente de produção real, talvez você queira relatar isso de outra forma
        # Mas para depuração, imprimir é útil. Não vamos setar db_pool para None aqui
        # para não mascarar o problema do acquire se a pool foi parcialmente criada ou similar.


@app.on_event("shutdown")
async def shutdown_db_pool():
    """Fecha a pool de conexão Oracle ao encerrar a aplicação."""
    global db_pool
    if db_pool:
        await db_pool.close()
        print("Pool de conexão Oracle fechada.")

# Função de dependência para obter uma conexão do banco de dados
async def get_db_connection():
    """Adquire uma conexão da pool e a libera no final da requisição."""
    connection = None
    try:
        if db_pool is None:
             # Se a pool não foi criada (erro na inicialização), levanta uma exceção
             raise HTTPException(status_code=500, detail="Database connection pool not initialized.")
        connection = await db_pool.acquire()
        yield connection # Fornece a conexão para o endpoint
    except Exception as e:
        # Tratar erros na aquisição da conexão
        print(f"Erro ao adquirir conexão do pool: {e}")
        raise HTTPException(status_code=500, detail=f"Erro interno ao conectar ao banco de dados: {e}")
    finally:
        if connection:
            await connection.release() # Garante que a conexão é sempre liberada


tabelas_permitidas = ["GS_CIDADE", "GS_BAIRRO", "GS_PREVISAO_TEMPO", "GS_FAMILIARES_DESAPARECIDOS", "GS_NIVEL_ALERTA"]

@app.get("/tabela/{nome_tabela}")
async def listar_tabela(nome_tabela: str, connection: oracledb.AsyncConnection = Depends(get_db_connection)):
    """Retorna todos os dados de uma tabela especificada."""
    nome_tabela = nome_tabela.upper()

    if nome_tabela not in tabelas_permitidas:
        # Se a tabela não é permitida, liberamos a conexão implicitamente pelo finally da dependência
        raise HTTPException(status_code=403, detail="Tabela não permitida")

    # Usar a conexão fornecida pela dependência
    async with connection.cursor() as cursor:
        try:
            # Usar binding para o nome da tabela para maior segurança, embora aqui seja uma lista fixa
            # Mas para consultas dinâmicas é crucial. Aqui f-string ainda é razoável pela lista fixa.
            await cursor.execute(f"SELECT * FROM {nome_tabela}")
            colunas = [col[0] for col in cursor.description]

            dados = []
            # Usar fetchall para obter todos os resultados de uma vez (cuidado com tabelas grandes)
            # Para tabelas muito grandes, considere fetchmany em um loop
            rows = await cursor.fetchall()

            for row in rows:
                linha = {}
                for col, val in zip(colunas, row):
                    # Tratamento para tipos de dados LOB (BLOB, CLOB)
                    if val is not None and hasattr(val, "read"):
                        val = await val.read()
                        if isinstance(val, bytes):
                            try:
                                # Tentar decodificar bytes para utf-8 se for o caso (CLOBs binários)
                                val = val.decode('utf-8')
                            except UnicodeDecodeError:
                                # Manter como bytes se não for decodificável (BLOBs)
                                pass

                    linha[col] = val
                dados.append(linha)

            # A conexão é liberada automaticamente pela dependência após o retorno
            return dados

        except Exception as e:
            # Tratar erros na execução da query
            print(f"Erro ao listar tabela {nome_tabela}: {e}")
            # A conexão será liberada pelo finally da dependência
            raise HTTPException(status_code=400, detail=f"Erro ao acessar dados da tabela {nome_tabela}: {e}")

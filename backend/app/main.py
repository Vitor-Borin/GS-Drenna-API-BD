from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
import oracledb
import os
from dotenv import load_dotenv
import asyncio

load_dotenv()

app = FastAPI()

# Permitir todas as origens por enquanto, ajustar conforme necessário
origins = [
    "*"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"], # Permitir todos os métodos, ajustar conforme necessário
    allow_headers=["*"], # Permitir todos os headers, ajustar conforme necessário
)

# Declarar db_pool como None globalmente
# Agora usaremos a pool SÍNCRONA
db_pool = None

@app.on_event("startup")
async def startup_db_pool():
    """Cria a pool de conexão Oracle (síncrona) ao iniciar a aplicação."""
    global db_pool
    try:
        # Usar create_pool para pool SÍNCRONA
        # Esta função NÃO é assíncrona, não precisa de await
        db_pool = oracledb.create_pool(
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
        print("Pool de conexão Oracle (síncrona) criada com sucesso!")
    except Exception as e:
        # Imprimir a exceção diretamente para depuração
        print(f"Erro ao criar pool de conexão Oracle: {e}")
        # Em um ambiente de produção real, talvez você queira relatar isso de outra forma


@app.on_event("shutdown")
async def shutdown_db_pool():
    """Fecha a pool de conexão Oracle ao encerrar a aplicação."""
    global db_pool
    if db_pool:
        # O close da pool síncrona TAMBÉM é síncrono, precisa rodar em thread pool se chamado de contexto assíncrono
        # No entanto, o evento de shutdown do FastAPI pode rodar código síncrono, então await pode não ser necessário aqui,
        # mas para garantir que não bloqueie, vamos usar to_thread.
        await asyncio.to_thread(db_pool.close)
        print("Pool de conexão Oracle (síncrona) fechada.")

# Função de dependência para obter uma conexão do banco de dados
# Esta dependência é assíncrona porque 'acquire' (mesmo na pool síncrona)
# precisa rodar em um thread para não bloquear o loop de eventos.
async def get_db_connection():
    """Adquire uma conexão (síncrona) da pool e a libera no final da requisição."""
    connection = None
    try:
        if db_pool is None:
             # Se a pool não foi criada (erro na inicialização), levanta uma exceção
             raise HTTPException(status_code=500, detail="Database connection pool not initialized.")
        # Adquirir a conexão da pool síncrona rodando em um thread
        connection = await asyncio.to_thread(db_pool.acquire)
        print(f"Tipo de objeto de conexão adquirido: {type(connection)}")
        yield connection # Fornece a conexão para o endpoint
    except Exception as e:
        # Tratar erros na aquisição da conexão
        print(f"Erro ao adquirir conexão do pool: {e}")
        raise HTTPException(status_code=500, detail=f"Erro interno ao conectar ao banco de dados: {e}")
    finally:
        if connection:
            # Liberar a conexão síncrona rodando em um thread
            await asyncio.to_thread(connection.release)


tabelas_permitidas = ["GS_CIDADE", "GS_BAIRRO", "GS_PREVISAO_TEMPO", "GS_FAMILIARES_DESAPARECIDOS", "GS_NIVEL_ALERTA"]

@app.get("/tabela/{nome_tabela}")
async def listar_tabela(nome_tabela: str, connection: oracledb.Connection = Depends(get_db_connection)):
    """Retorna todos os dados de uma tabela especificada (usando conexão síncrona em thread)."""
    nome_tabela = nome_tabela.upper()

    if nome_tabela not in tabelas_permitidas:
        # Se a tabela não é permitida, a conexão será liberada pela dependência
        raise HTTPException(status_code=403, detail="Tabela não permitida")

    # As operações de banco de dados SÍNCRONAS precisam rodar em um thread
    # para não bloquear o loop de eventos assíncrono do FastAPI.
    # Usamos asyncio.to_thread para executar código síncrono em um thread separado.
    try:
        # Criar cursor síncrono em thread
        cursor = await asyncio.to_thread(connection.cursor)
        try:
            # Executar query síncrona em thread
            await asyncio.to_thread(cursor.execute, f"SELECT * FROM {nome_tabela}")

            # Obter descrição das colunas sincrona em thread
            colunas_desc = await asyncio.to_thread(lambda: cursor.description)
            colunas = [col[0] for col in colunas_desc] if colunas_desc else []

            dados = []
            # Buscar todos os resultados sincrono em thread
            rows = await asyncio.to_thread(cursor.fetchall)

            for row in rows:
                linha = {}
                for col, val in zip(colunas, row):
                    # Tratamento para tipos de dados LOB (BLOB, CLOB) - a leitura também pode ser síncrona ou assíncrona dependendo da API e versão
                    # Para garantir compatibilidade com a conexão SÍNCRONA, assumimos leitura SÍNCRONA
                    if val is not None and hasattr(val, "read"):
                         # Se a leitura do LOB for assíncrona (AsyncLOB), precisamos awaits.
                         # Se for síncrona (LOB), precisamos asyncio.to_thread
                         # Com a pool síncrona, é mais provável que seja um LOB síncrono.
                         # Vamos usar asyncio.to_thread para a leitura para sermos seguros.
                        try:
                            val = await asyncio.to_thread(val.read)
                            if isinstance(val, bytes):
                                try:
                                    val = val.decode('utf-8')
                                except UnicodeDecodeError:
                                    pass # Manter como bytes se não decodificar
                        except Exception as read_error:
                             print(f"Erro ao ler LOB para coluna {col}: {read_error}")
                             val = None # Define como None ou trata o erro adequadamente

                    linha[col] = val
                dados.append(linha)

            # Fechar cursor síncrono em thread
            await asyncio.to_thread(cursor.close)

            # A conexão é liberada automaticamente pela dependência após o retorno
            return dados

        except Exception as e:
            # Certificar que o cursor é fechado em caso de erro antes de levantar a exceção
            if cursor:
                try:
                    await asyncio.to_thread(cursor.close)
                except Exception as close_error:
                    print(f"Erro ao fechar cursor após erro na query: {close_error}")

            # Tratar erros na execução da query
            print(f"Erro ao listar tabela {nome_tabela}: {e}")
            # A conexão será liberada pelo finally da dependência
            raise HTTPException(status_code=400, detail=f"Erro ao acessar dados da tabela {nome_tabela}: {e}")
    except Exception as e:
        # Captura erros na criação do cursor ou outras operações antes da query
        print(f"Erro durante o processamento da requisição para {nome_tabela}: {e}")
        raise HTTPException(status_code=500, detail=f"Erro interno no servidor: {e}")

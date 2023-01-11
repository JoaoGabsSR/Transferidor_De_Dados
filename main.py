from pymysql import cursors, connect
import csv

dados = []


def conexao_db(host='', user='', password='', database='', charset=''):
    """
    Realiza conexão com o banco de dados.

    Args:
        host (str, optional): Host o qual se deve realizar a conexão. Defaults to ''.
        user (str, optional): Usuário de conexão ao banco de dados. Defaults to ''.
        password (str, optional): Senha de conexão com o banco de dados. Defaults to ''.
        database (str, optional): Banco de dados ao qual vai se conectar. Defaults to ''.
        charset (str, optional): Formatação do banco de dados. Defaults to ''.

    Returns:
        conexao: Conexão já realizada com o banco de dados
        e: Caso conexão falhe retorna erro
    """
    try:
        conexao = connect(
            host=host,
            user=user,
            password=password,
            database=database,
            charset=charset,
            cursorclass=cursors.DictCursor
        )
    except Exception as e:
        return e
    else:
        return conexao


def exportar_csv(nome_arquivo='dados.csv'):
    """
    Exporta os dados de um banco de dados para um arquivo .csv

    Args:
        nome_arquivo (str, optional): Nome para o arquivo .csv criado. Defaults to 'dados.csv'.
    """

    conn = conexao_db('host', 'user', 'password', 'database', 'charset')

    dados.clear()

    with conn.cursor() as cursor:
        query = "SELECT * FROM `tb_livros`"
        cursor.execute(query)
        query_result = cursor.fetchall()

        for result in query_result:
            dados.append(result)

    print('Dados coletados do banco com sucesso;')

    a = open(nome_arquivo, 'w', newline='', encoding='utf-8')
    w = csv.writer(a, delimiter=';')

    # !IMPORTANTE: esse linha inseri a primeira linha do arquivo .csv, nela contém as colunas da tabela, deve ser adaptada a tabela do banco de dados #
    # w.writerow(['id', 'nome', 'autor', 'sinopse', 'genero'])

    # !IMPORTANTE: "for" deve ser adaptada a cada tabela #
    for r in dados:
        w.writerow([f"{r['id']}", f"{r['nome']}", f"{r['autor']}", f"{r['sinopse']}", f"{r['genero']}"])

    print('Dados escritos no arquivo .csv com sucesso;')

    a.close()

    print('Dados exportados com sucesso;')


def importar_csv(nome_arquivo='dados.csv'):
    """
    Importa os dados de um arquivo .csv para um banco de dados

    Args:
        nome_arquivo (str, optional): Nome do arquivo .csv a ser importado. Defaults to 'dados.csv'.
    """
    conn = conexao_db('host', 'user', 'password', 'database', 'charset')

    dados.clear()

    with open(nome_arquivo, encoding='utf-8') as arquivo:
        tabela = csv.reader(arquivo, delimiter=';')

        # !IMPORTANTE: "for" deve ser adaptada a cada tabela #
        for r in tabela:
            dados.append({
                'id': r[0],
                'nome': r[1],
                'autor': r[2],
                'sinopse': r[3],
                'genero': r[4]
            })

    print('Dados do arquivo .csv coletados com sucesso;')

    # !IMPORTANTE: adaptar as querys para cada banco #
    with conn.cursor() as cursor:
        query_truncate = "TRUNCATE TABLE `tb_teste`;"
        cursor.execute(query_truncate)
        conn.commit()

        print('Truncate feito com sucesso')

        for d in dados:
            query_insert = "INSERT INTO `tb_teste` (`id`, `nome`, `autor`, `sinopse`, `genero`) VALUES (%s, %s, %s, %s, %s)"
            cursor.execute(query_insert, (d['id'], d['nome'], d['autor'], d['sinopse'], d['genero']))
            conn.commit()

        print('Dados importados com sucesso')


def processo_completo():
    """
    Realiza o processo de exportação de uma tabela, e já importa para outra
    """
    exportar_csv()
    importar_csv()


processo_completo()

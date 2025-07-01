import logging
import os
from openai import AzureOpenAI
from dotenv import load_dotenv
from fastapi import APIRouter, HTTPException
import json
from azure.storage.blob import BlobServiceClient
import traceback

load_dotenv()
router = APIRouter()
 
# Configuração do logger para salvar em um arquivo  
logger = logging.getLogger(__name__)  
logger.setLevel(logging.INFO)  
  
# Cria um manipulador de arquivo  
file_handler = logging.FileHandler('log.txt', encoding='utf-8')  
file_handler.setLevel(logging.INFO)  
  
# Cria um formatador e o adiciona ao manipulador de arquivo  
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')  
file_handler.setFormatter(formatter)  
  
# Adiciona o manipulador de arquivo ao logger  
logger.addHandler(file_handler) 

# Função para fazer o upload do log para o Blob Storage  
def upload_log_to_blob():  
    try:  
        connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        if not connection_string:  
            raise ValueError("A variável de ambiente AZURE_STORAGE_CONNECTION_STRING não está definida.") 
          
        blob_service_client = BlobServiceClient.from_connection_string(conn_str=connection_string)  
           
        container_name = "logs"
        blob_name = "log"   

        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)    
  
        with open("log.txt", "rb") as data:  
            blob_client.upload_blob(data, overwrite=True)
        logger.info("Upload do log.txt realizado com sucesso.")  
          
    except Exception as e:  
        logger.error(f"Erro ao fazer upload do log para o blob: {e}")  
        print(f"Erro ao fazer upload do log para o blob: {e}") 

# Chamar a função para realizar o upload do log 
upload_log_to_blob()  

@router.post("/summarize_case1_teste/")
async def summarize_text(text_to_summarize: dict):
    try:
        logger.info(f'[CASE 1 - Iniciando processo de sumarização]')  
        text = agrupar_similares(desconsiderar_perdas_menores(calcular_soma_quantidade(text_to_summarize)))
        client = AzureOpenAI(azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT1"), api_key=os.getenv("AZURE_OPENAI_API_KEY1"), api_version="2024-02-01")
        prompt = '''
        Como Entrada, você irá receber informações no formato JSON, que contém informações de produção 5 Usinas da empresa Vale.
        
        O JSON de Entrada terá o seguinte formato:
            {
                "regiao": "Nome da regiao onde estão localizadas as Usinas",
                "usinas": [
                    {
                        "usina": "USINA I - Identificação da Usina, vai de I até V",
                        "total_produzido": Valor total produzido separadamente por cada Usina identificada de I até V. Esse valor pode ser Positivo ou Negativo.,
                        "eventos": [
                            {
                            "quantidade": Quantidade de produção da Usina. Esse valor pode ser Positivo ou Negativo,
                            "justificativa": "Justificativa para a quantidade de produdação da Usina. Estará preenchido somente se Quantidade for Negativo"
                            }
                        ]
                    }
                ]
            }

        Como Saida, preciso que você gere outro JSON no formato abaixo:
            {
                "regiao": "Deverá ser o MESMO valor informado no JSON de Entrada no campo 'regiao'",
                "usinas": [
                    {
                        "usina": "Identificação da USINA, conforme informado no JSON de Entrada no campo 'usina'",
                        "comentarios": "Siga as instruções (1) abaixo para preencher este campo."
                    },
                    {
                        "usina": "Identificação da USINA, conforme informado no JSON de Entrada no campo 'usina'",
                        "comentarios": "Siga as instruções (1) abaixo para preencher este campo."
                    }
                ],
                "resumo_executivo": "Siga as instruções (2) abaixo para preencher este campo."
            }

            Instruções para gerar o campo COMENTÁRIOS:
            1. Usinas com o valor do campo 'total_produzido' positivo: preencha o campo 'comentarios' com 'Não houveram perdas significativas'.
            2. Verifique as 3 maiores perdas da usina, considerando os 3 maiores valores negativos do campo 'quantidade' do JSON de entrada. 
            3. Usinas com o valor do campo 'total_produzido' negativo: preencha o campo 'comentarios' com sua análise dos eventos de cada USINA e gere um resumo considerando as 3 maiores perdas daquela usina específica.
            4. Seu resumo deve ser completo, citando os nomes tecnicos e abreviações, sem deixar de fora informações relevantes.
            5. Não é necessário trazer o valor das perdas no campo 'comentarios', essa informação deverá ser omitida.
            6. Não é necessário trazer o nome da usina, essa informação deverá ser omitida.
            7. Considere somente as 3 perdas mais significativas (ou seja, com o maior número negativo), você deve omitir as perdas menos significativas.
            8. Não copie o texto do json de entrada. Você deve escrever o comentário de forma coesa e coerente.
            9. Não é necessário numerar as perdas.
            10. Nunca traduza as siglas e abreviações tecnicas usando o glossário no campo 'comentarios', apenas use o glossario para usar as preposições corretas com as siglas.
            11. Nunca escreva: 'no transportador TR301010'. Escreva: 'no TR301010'

            Instruções para gerar o campo RESUMO_EXECUTIVO:
            1. Faça um resumo executivo das 3 maiores perdas entre todas as usinas. 
            2. Comece o resumo com "Impacto devido à". 
            3. Use uma linguagem simples e mais acessível no preenchimento deste campo, 
            4. Não use siglas e abreviações técnicas no campo 'resumo_executivo'. 
            5. Consulte o glossário abaixo para entender e substituir os termos:
                Alimentadores: AL
                Balanças: BL
                Bombas: BA
                Britadores: BR
                Caixas/Tanques/Reservatórios: TQ
                Empilhadeiras: EP
                Empilhadeira e Recuperadora: ER
                Espessadores: ES
                Peneiras: PN
                Recuperadores: RP
                Subestações: SE
                Transportadores: TR
                Tripper: TP
                Pá carregadeiras: PC
                Escavadeiras: ESC
                Caminhões: CA
                Peneiramento Primário: PEN I ou PEN01 ou PEN 1
                Peneiramento Secundário: PEN II ou PEN02 ou PEN 2
                Peneiramento Terciário: PEN III ou PEN03 ou PEN 3
                Britagem Primária: BRIT
                Britagem Secundária: BRIT II
                Britagem Terciária: BRIT III
                Repeneiramento 2: REP2
                Repeneiramento 3: REP3
                Repeneiramento 6: REP6
                5a Britagem: MCR11 ou MCR13 ou SLM 901 - Sistema 1
                SLM 902 - Sistema 3: BM11, BM12, BM13, BM901, BM902
                Manutenção Preventiva: MP
                Parada Geral da Usina: PGU
                Disponibilidade Física: DF
                Concentração Magnética: CM
            
            Formato da resposta: Nunca coloque ```json ou ``` na sua resposta. Retorne apenas o JSON, nada mais deverá ser incluido.
            
            JSON de Entrada:
            {text}
        '''
        glossario = "{\"Alimentadores\": \"AL\", \"Balanças\": \"BL\", \"Bombas\": \"BA\", \"Britadores\": \"BR\", \"Caixas/Tanques/Reservatórios\": \"TQ\", \"Empilhadeiras\": \"EP\", \"Empilhadeira e Recuperadora\": \"ER\", \"Espessadores\": \"ES\", \"Peneiras\": \"PN\", \"Recuperadoras\": \"RP\", \"Recuperadoras\": \"RC\", \"Subestações\": \"SE\", \"Transportadores\": \"TR\", \"Tripper\": \"TP\", \"Pá carregadeiras\": \"PC\", \"Escavadeiras\": \"ESC\", \"Escavadeiras\": \"EC\", \"Caminhões\": \"CA\", \"Peneiramento Primário\": \"PEN I\", \"Peneiramento Secundário\": \"PEN II\", \"Peneiramento Terciário\": \"PEN III\", \"Britagem Primária\": \"BRIT\", \"Britagem Secundária\": \"BRIT II\", \"Britagem Terciária\": \"BRIT III\", \"Repeniramento 2\": \"REP2\", \"Repeniramento 3\": \"REP3\", \"Repeniramento 5\": \"REP5\", \"Repeniramento 6\": \"REP6\", \"5a Britagem\": \"5a B\", \"MCR11 ou\": \"BM11\", \"MCR13 ou\": \"BM12\", \"MCR13 ou\": \"BM13\", \"SLM 901 - Sistema 1\": \"BM901\", \"SLM 902 - Sistema 3\": \"BM902\", \"Manutenção Preventiva\": \"MP\", \"Parada Geral da Usina\": \"PGU\", \"Disponibilidade Física\": \"DF\", \"Concentração Magnética\": \"CM\"}" 
        prompt = prompt.replace("{text}", str(text))
        # Requisição para a API da OpenAI
        logger.info("Iniciando requisição para a API da OpenAI.")
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "Atue como um Analista Sênior de Produção com especialização em Mineração da empresa brasileira Vale."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=2000,
            n=1,
            stop=None,
            temperature=0.01,
        )
        # Extrair o texto sumarizado da resposta
        summary = response.choices[0].message.content
        logger.info("Sumarização realizada com sucesso.")
        return format_response(summary)
        return text
    except Exception as e:
        logger.error(f'Erro na função summarize_text: {str(e)}')
        stack_trace = traceback.format_exc()
        logger.error(stack_trace)
        raise HTTPException(status_code=500, detail="Sistema indisponível no momento. Tente novamente em alguns minutos.")
    
def format_response(summary: str) -> dict:  
    try:  
        logger.info("Iniciando formatação da resposta")
        json_response = json.loads(summary)
        logger.info("Formatação da resposta concluída com êxito")  
        return json_response  
    except json.JSONDecodeError as e: 
        logger.error(f'Erro ao formatar a resposta: {str(e)}') 
        raise HTTPException(status_code=500, detail=f"Failed to decode JSON response: {str(e)}") 
    
def calcular_soma_quantidade(data): 
    logger.info("Iniciando cálculo do total produzido") 
    for usina in data['usinas']:   
        soma_quantidade = sum(evento['quantidade'] for evento in usina['eventos'])  
        usina["total_produzido"] = soma_quantidade  

    logger.info("Cálculo do total produzido realizado com sucesso")    
    return data   
            
def desconsiderar_perdas_menores(data):
    logger.info("Iniciando função para desconsiderar perdas menores") 
    for usina in data["usinas"]:
        if usina["total_produzido"] > -500:
            usina["total_produzido"] = 100
            usina["eventos"] = []

    logger.info("Verificação para desconsiderar perdas menores realizada com sucesso")        
    return data
            
def agrupar_similares(data): 
    logger.info("Iniciando função para agrupar itens similares")  
    # Dicionário para armazenar grupos de itens com observações semelhantes  
    grouped_data = {  
        "regiao": data["regiao"],
        "usinas": []  
    }  
  
    for usina in data["usinas"]:  
        usina_dict = {  
            "usina": usina["usina"],
            "total_produzido": usina["total_produzido"],  
            "eventos": []  
        }  
  
        # Dicionário para agrupar eventos com a mesma justificativa  
        eventos_agrupados = {}  
  
        for evento in usina["eventos"]:    
            justificativa = evento["justificativa"]  
            quantidade = evento["quantidade"]  
  
            if justificativa:  # Agrupa apenas eventos com justificativa  
                if justificativa not in eventos_agrupados:  
                    eventos_agrupados[justificativa] = {  
                        "quantidade": quantidade,  
                        "justificativa": justificativa  
                    }  
                else:  
                    eventos_agrupados[justificativa]["quantidade"] += quantidade  
            else:   
                if quantidade < 0:
                # Se não houver justificativa, adiciona o evento como está  
                    usina_dict["eventos"].append(evento)  
  
        # Adiciona os eventos agrupados  
        for evento in eventos_agrupados.values():  
            usina_dict["eventos"].append(evento)  
            
        # Ordena os eventos da menor quantidade para a maior  
        usina_dict["eventos"] = sorted(usina_dict["eventos"], key=lambda x: x["quantidade"])  
  
        grouped_data["usinas"].append(usina_dict)    
  
    logger.info("Agrupamento de itens similares realizado com sucesso")
    return grouped_data 
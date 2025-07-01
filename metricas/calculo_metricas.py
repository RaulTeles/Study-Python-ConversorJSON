import json
from rouge_score import rouge_scorer
from bert_score import score
import numpy as np
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import pandas as pd
import os
import time

# Case 1
def extrai_json_entrada(json_contexto):  
    print('Iniciando Extração JSON')  
    # Carregar o arquivo JSON  
    file_path = json_contexto  
    with open(file_path, 'r', encoding='utf-8') as file:  
        data = json.load(file)  
      
    # Inicializar a variável que irá armazenar o conteúdo concatenado  
    all_content = ""  
      
    # Percorrer as usinas e eventos  
    for usina in data['usinas']:  
        all_content += '\n\n' + usina['usina'] + '\n'
        if usina['total_produzido'] == 100:
            all_content += 'Não houveram perdas significativas'
        else:
            contador = 0
            for evento in usina['eventos']:  
                # Concatenar a justificativa se existir  
                if evento['justificativa']:  
                    all_content += evento['justificativa'] + " "  
                    contador += 1
                    if contador >= 3:
                        break
      
    # Remover espaços extras no início e fim da string  
    all_content = all_content.strip()  
    
    # Retornar a variável all_content conforme necessário  
    return all_content  

def extrai_json_saida(json_contexto):  
    print('Iniciando Extração JSON')  
    # Carregar o arquivo JSON  
    file_path = json_contexto  
    with open(file_path, 'r', encoding='utf-8') as file:  
        data = json.load(file)  
      
    # Inicializar a variável que irá armazenar o conteúdo concatenado  
    all_content = ""  
      
    # Percorrer as usinas e eventos  
    for usina in data['usinas']:   
        all_content += '\n\n' + usina['usina'] + '\n' 
        if usina['comentarios']:  
            all_content += usina['comentarios'] + " "  
      
    # Remover espaços extras no início e fim da string  
    all_content = all_content.strip()
    #all_content += data['resumo_executivo']  
      
    # Retornar a variável all_content conforme necessário  
    return all_content  

# Case 2
""" def extrai_json_entrada(json_contexto):  
    print('Iniciando Extração JSON')  
    # Carregar o arquivo JSON  
    file_path = json_contexto  
    with open(file_path, 'r', encoding='utf-8') as file:  
        data = json.load(file)  
      
    # Inicializar a variável que irá armazenar o conteúdo concatenado  
    all_content = ""  
      
    # Percorrer as usinas e eventos  
    for sistema in data['sistemas']:  
        if data['tipo'] == 'ROM':
            all_content += '\n\n'+ sistema['sistema'] +'\n' + str(sistema['totalSistema'])
            for equipamento in sistema['equipamentos']:
                all_content += '\n\n'+ equipamento['equipamento'] +'\n' + str(equipamento['totalEquipamento'])
                for indicador in equipamento['indicadores']:
                    all_content += '\n' + indicador['tipo'] + '\n' + str(indicador['totalIndicador'])
                    for detalhe in indicador['detalhes']:
                        all_content += '\n' + str(detalhe['perda']) + ' ' + detalhe['causa'] + ' ' + detalhe['falha'] + ' ' + detalhe['observacao']
        else:
            all_content += '\n\n'+ sistema['sistema'] +'\n' + str(sistema['totalSistema'])
            for equipamento in sistema['equipamentos']:
                for indicador in equipamento['indicadores']:
                    all_content += '\n' + indicador['tipo'] + '\n' + str(indicador['totalIndicador'])
                    for detalhe in indicador['detalhes']:
                        all_content += '\n' + str(detalhe['perda']) + ' ' + detalhe['causa'] + ' ' + detalhe['falha'] + ' ' + detalhe['observacao']
      
    # Remover espaços extras no início e fim da string  
    all_content = all_content.strip()  
    
    # Retornar a variável all_content conforme necessário  
    return all_content  

def extrai_json_saida(json_contexto):  
    print('Iniciando Extração JSON')  
    # Carregar o arquivo JSON  
    file_path = json_contexto  
    with open(file_path, 'r', encoding='utf-8') as file:  
        data = json.load(file)  
      
    # Inicializar a variável que irá armazenar o conteúdo concatenado  
    all_content = ""  
      
    # Percorrer as usinas e eventos  
    for sistema in data['buildUp_tecnico']:  
        if data['tipo'] == 'ROM':
            all_content += '\n\n'+ sistema['sistema'] +'\n' + str(sistema['total_sistema'])
            for equipamento in sistema['equipamentos']:
                all_content += '\n\n'+ equipamento['equipamento'] +'\n' + str(equipamento['total_equipamento'])
                for indicador in equipamento['indicadores']:
                    all_content += '\n' + indicador['tipo'] + '\n' + str(indicador['total_indicador'])
                    for detalhe in indicador['detalhes']:
                        all_content += '\n' + str(detalhe['saldo']) + ' ' + detalhe['justificativa']
        else:
            all_content += '\n\n'+ sistema['sistema'] +'\n' + str(sistema['total_sistema'])
            for indicador in sistema['indicadores']:
                all_content += '\n' + indicador['tipo'] + '\n' + str(indicador['total_indicador'])
                for detalhe in indicador['detalhes']:
                    all_content += '\n' + str(detalhe['saldo']) + ' ' + detalhe['justificativa']
      
    # Remover espaços extras no início e fim da string  
    all_content = all_content.strip()
      
    # Retornar a variável all_content conforme necessário  
    return all_content   """




# ---------------------- ROUGE --------------------------------
# Instalar Bibliotecas PyTorch e Rouge: pip install torch rouge
# -------------------------------------------------------------


def calculate_rouge(reference: str, hypothesis: str):
    """
    Calculate ROUGE scores between a hypothesis summary and a reference summary.
    
    Args:
    hypothesis (str): The summary generated by the model.
    reference (str): The reference summary.
    
    Returns:
    tuple: A tuple with precision, recall, and f-measure for ROUGE-1, ROUGE-2, and ROUGE-L.
    """
    print('Iniciando ROUGE')
    scorer = rouge_scorer.RougeScorer(['rouge1', 'rouge2', 'rougeL'], use_stemmer=True)
    scores = scorer.score(reference, hypothesis)
    
    rouge1_precision = scores['rouge1'].precision
    rouge1_recall = scores['rouge1'].recall
    rouge1_fmeasure = scores['rouge1'].fmeasure
    
    rouge2_precision = scores['rouge2'].precision
    rouge2_recall = scores['rouge2'].recall
    rouge2_fmeasure = scores['rouge2'].fmeasure
    
    rougeL_precision = scores['rougeL'].precision
    rougeL_recall = scores['rougeL'].recall
    rougeL_fmeasure = scores['rougeL'].fmeasure
    
    return rouge1_precision, rouge1_recall, rouge1_fmeasure, rouge2_precision, rouge2_recall, rouge2_fmeasure, rougeL_precision, rougeL_recall, rougeL_fmeasure

# ------------------ BERT SCORE ------------------------
# Instalar Biblioteca BERT Score: pip install bert_score
# ------------------------------------------------------

 
def BERTScore(references, hypotheses):
    print('Iniciando BERT')
    #Exemplo 2:
    # Hipóteses e referências
    #hypotheses = ["Um gato preto está dormindo no sofá"]
    #references = ["Há um gato preto dormindo no sofá"]

    hypotheses = [hypotheses]
    references = [references]

    # Calculando BERTScore especificando o idioma como português
    P, R, F1 = score(hypotheses, references, lang='pt')  # 'pt' para português

    # Convertendo tensores para valores escalares
    precision_value = P[0].item()
    recall_value = R[0].item()
    f1_value = F1[0].item()

    # Retornando apenas os valores numéricos
    return precision_value, recall_value, f1_value


# ------------------------ SUPERT ------------------------------
# Instalar Biblioteca SUPERT: pip install sklearn
# --------------------------------------------------------------
 

def supert_score(reference_summaries, generated_summary):
    """
    Calculate the SUPERT score for a generated summary compared to a set of reference summaries.
    
    :param generated_summary: The summary generated by the model.
    :param reference_summaries: A list of human reference summaries.
    :return: SUPERT score.
    """
    print('Iniciando SUPERT')
    # Combine generated summary and reference summaries into a single list
    all_summaries = [generated_summary] + [reference_summaries]
    
    # Convert summaries to vectors using CountVectorizer
    vectorizer = CountVectorizer().fit_transform(all_summaries)
    vectors = vectorizer.toarray()
    
    # Calculate cosine similarity between the generated summary and each reference summary
    cosine_similarities = cosine_similarity(vectors[0:1], vectors[1:]).flatten()
    
    # The SUPERT score is the average cosine similarity
    supert_score = np.mean(cosine_similarities)
    
    return supert_score


tempo_total_inicio = time.time()

sprint = 'Teste'

arquivo_resultado = f'metricas-{sprint}.xlsx'

if not os.path.exists(arquivo_resultado):
    # Se não existir, crie o arquivo com os headers
    headers = [
        'tipo', 'rouge1_precision', 'rouge1_recall', 'rouge1_fmeasure',
        'rouge2_precision', 'rouge2_recall', 'rouge2_fmeasure', 'rougeL_precision',
        'rougeL_recall', 'rougeL_fmeasure', 'bert_precision', 'bert_recall', 'bert_f1', 'supert_score'
    ]
    """ headers = [
        'tipo', 'rouge1_precision',
        'rouge2_precision', 'rougeL_precision',
        'bert_precision'
    ] """
    pd.DataFrame(columns=headers).to_excel(arquivo_resultado, index=False)


dias = ['05-06-2024', '08-06-2024', '17-06-2024']
tipos = ['esteril', 'producao', 'ROM']

def log(mensagem, num):
    if num == 1:
        with open('log_e.txt', 'w') as arquivo_texto:
            arquivo_texto.write(mensagem)
    if num == 2:
        with open('log_s.txt', 'w') as arquivo_texto:
            arquivo_texto.write(mensagem)
    

for dia in dias:
    print(f'INICIANDO TEMA {dia}')

    iteracao_inicio = time.time()

    contexto = extrai_json_entrada(f'case1/entrada/{dia}-Informações COI - Usina.json')
    log(contexto, 1)
    print(f'Concatenação de Contexto OK')

    sumarizacao = extrai_json_saida(f'case1/saida_openai/{dia}-Saida.json')
    log(sumarizacao, 2)
    print(f'Leitura de Sumarização OK')
    
    """ sumarizacao = extrai_json_saida(f'case1/saida_plataforma/{dia}-Saida.json')
    log(sumarizacao, 2)
    print(f'Leitura de Sumarização OK') """
    
    """ contexto = extrai_json_entrada(f'case2/entrada/case02_inputs_completo_{dia}.json')
    log(contexto, 1)
    print(f'Concatenação de Contexto OK')

    sumarizacao = extrai_json_saida(f'case2/saida/case02_outputs_completo_{dia}.json')
    log(sumarizacao, 2)
    print(f'Leitura de Sumarização OK') """

    rouge1_precision, rouge1_recall, rouge1_fmeasure, rouge2_precision, rouge2_recall, rouge2_fmeasure, rougeL_precision, rougeL_recall, rougeL_fmeasure = calculate_rouge(sumarizacao, contexto)
    print(f'Print das estatísticas ROUGE: {rouge1_precision, rouge1_recall, rouge1_fmeasure, rouge2_precision, rouge2_recall, rouge2_fmeasure, rougeL_precision, rougeL_recall, rougeL_fmeasure}')

    bert_p, bert_r, bert_f1 = BERTScore(sumarizacao, contexto)
    print(f'Print das estatísticas BERT: {bert_p, bert_r, bert_f1}')

    score_supert = supert_score(sumarizacao, contexto)
    print(f'Print das estatísticas SUPERT: {score_supert}')

    resultado = {
        'tema': dia,
        'rouge1_precision': rouge1_precision,
        'rouge1_recall': rouge1_recall,
        'rouge1_fmeasure': rouge1_fmeasure,
        'rouge2_precision': rouge2_precision,
        'rouge2_recall': rouge2_recall,
        'rouge2_fmeasure': rouge2_fmeasure,
        'rougeL_precision': rougeL_precision,
        'rougeL_recall': rougeL_recall,
        'rougeL_fmeasure': rougeL_fmeasure,
        'bert_precision': bert_p,
        'bert_recall': bert_r,
        'bert_f1': bert_f1,
        'supert_score': score_supert
    }

    # Converta o dicionário em um DataFrame
    resultado_df = pd.DataFrame([resultado])
    
    # Adicione o DataFrame ao arquivo Excel
    with pd.ExcelWriter(arquivo_resultado, mode='a', engine='openpyxl', if_sheet_exists='overlay') as writer:
        resultado_df.to_excel(writer, index=False, header=False, startrow=writer.sheets['Sheet1'].max_row)

    iteracao_fim = time.time()
    tempo_iteracao = iteracao_fim - iteracao_inicio
    print(f"Iteração {dia} concluída em {tempo_iteracao:.2f} segundos")
    

tempo_total_fim = time.time()
tempo_total_execucao = tempo_total_fim - tempo_total_inicio
print(f"Processamento total concluído em {tempo_total_execucao:.2f} segundos")
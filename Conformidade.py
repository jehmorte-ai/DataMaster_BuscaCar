# -*- coding: utf-8 -*-
"""
Created on Fri Aug 15 14:24:44 2025

@author: Jessica
"""

# conformidade_mapping.py
import pandas as pd, re, glob, os
from unidecode import unidecode
from rapidfuzz import fuzz, process
from datetime import datetime

# pastas onde estão os CSVs
PASTA_FIPE  = r"C:\Users\Jessica\Desktop\DataMaster\Cargas_fipe"
PASTA_SUSEP = r"C:\Users\Jessica\Desktop\DataMaster\Cargas_Susep"
PASTA_OUT   = r"C:\Users\Jessica\Desktop\DataMaster\Conformidade"
os.makedirs(PASTA_OUT, exist_ok=True)

# pega os arquivos mais recentes
def mais_recente(pasta, padrao="*.csv"):
    arqs = glob.glob(os.path.join(pasta, padrao))
    if not arqs: raise FileNotFoundError(f"Sem CSVs em {pasta}")
    return max(arqs, key=os.path.getctime)

FIPE_CSV  = mais_recente(PASTA_FIPE)
SUSEP_CSV = mais_recente(PASTA_SUSEP, padrao="susep_normalizado_*.csv")
print("FIPE:", FIPE_CSV)
print("SUSEP:", SUSEP_CSV)

# thresholds
TH_ALTO, TH_MEDIO = 85, 70

APELIDOS_MARCA = {
    "VW":"VOLKSWAGEN","VOLKS":"VOLKSWAGEN",
    "GM":"CHEVROLET","CHEV":"CHEVROLET",
    "MB":"MERCEDES-BENZ","MERCEDES":"MERCEDES-BENZ",
    "CITROËN":"CITROEN","CITROEN":"CITROEN"
}

def normalize_text(s):
    if pd.isna(s): return ""
    s = unidecode(str(s)).upper().strip()
    s = re.sub(r"[^A-Z0-9 ]"," ", s)
    s = re.sub(r"\s+"," ", s)
    return s

def normaliza_marca(m):
    m = normalize_text(m)
    if m in ("VOLKS","VW","VOLKSWAGEN"): m = "VOLKSWAGEN"
    if m in ("GM","CHEVROLET","CHEV"):   m = "CHEVROLET"
    if m in ("MERCEDES","MERCEDES BENZ","MERCEDES-BENZ","MB"): m = "MERCEDES-BENZ"
    if m in ("CITROEN","CITROËN"): m = "CITROEN"
    return APELIDOS_MARCA.get(m, m)

STOP = {"16V","8V","FLEX","MT","AT","CVT","TURBO","TSI","MPI","GASOLINA","ALCOOL","ETANOL","DIESEL","HIBRIDO","HYBRID"}
def modelo_chave(s):
    s = normalize_text(s)
    tokens = [t for t in s.split() if t not in STOP]
    return " ".join(tokens)

# carrega
fipe = pd.read_csv(FIPE_CSV)
susep = pd.read_csv(SUSEP_CSV)

# garante nomes
fipe.columns = [c.lower() for c in fipe.columns]
susep.columns = [c.lower() for c in susep.columns]

# prepara dimensões
fipe["marca_norm"]  = fipe["marca"].apply(normaliza_marca)
fipe["modelo_norm"] = fipe["modelo"].apply(modelo_chave)
fipe_dim = fipe[["marca_norm","modelo_norm","marca","modelo"]].drop_duplicates().reset_index(drop=True)

# susep deve ter 'marca' e 'modelo_susep' (do script 1)
susep["marca_norm"]  = susep["marca"].apply(normaliza_marca)
susep["modelo_norm"] = susep["modelo_susep"].apply(modelo_chave)

def best_fipe_match(su_marca_n, su_modelo_n):
    marcas = fipe_dim["marca_norm"].unique().tolist()
    cand_marcas = process.extract(su_marca_n, marcas, scorer=fuzz.WRatio, limit=5) or []
    melhor = None; melhor_score = -1
    if not cand_marcas:
        # fallback: compara contra todas marcas
        cand_marcas = [(m, 0, i) for i, m in enumerate(marcas[:5])]
    for marca_norm, score_marca, _ in cand_marcas:
        sub = fipe_dim[fipe_dim["marca_norm"] == marca_norm]
        if sub.empty: 
            continue
        mm = process.extractOne(su_modelo_n, sub["modelo_norm"].tolist(), scorer=fuzz.WRatio)
        if not mm: 
            continue
        modelo_norm, score_modelo, pos = mm
        score_total = 0.6*score_marca + 0.4*score_modelo
        if score_total > melhor_score:
            melhor_score = score_total
            linha = sub.iloc[pos]
            melhor = (linha, score_total, score_marca, score_modelo)
    return melhor

rows = []
for _, r in susep.iterrows():
    su_marca, su_modelo = r.get("marca",""), r.get("modelo_susep","")
    res = best_fipe_match(r["marca_norm"], r["modelo_norm"])
    if not res:
        rows.append({
            "susep_marca": su_marca, "susep_modelo": su_modelo,
            "fipe_marca": None, "fipe_modelo": None,
            "score_total": 0, "score_marca": 0, "score_modelo": 0,
            "status": "REVISAR", "metodo": "no_candidates"
        })
    else:
        linha, st, sm, smodelo = res
        status = "MATCH_ALTO" if st >= TH_ALTO else ("REVISAR_RAPIDO" if st >= TH_MEDIO else "REVISAR")
        rows.append({
            "susep_marca": su_marca,
            "susep_modelo": su_modelo,
            "fipe_marca": linha["marca"],
            "fipe_modelo": linha["modelo"],
            "score_total": round(st,2),
            "score_marca": round(sm,2),
            "score_modelo": round(smodelo,2),
            "status": status,
            "metodo": "brand+model_composite"
        })

df_map = pd.DataFrame(rows).sort_values(["status","score_total"], ascending=[True,False]).reset_index(drop=True)
ARQ_OUT = os.path.join(PASTA_OUT, f"mapeamento_susep_fipe_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
df_map.to_csv(ARQ_OUT, index=False, encoding="utf-8-sig")
print(f" Mapeamento gerado: {ARQ_OUT}")
print(df_map.head(15).to_string(index=False))
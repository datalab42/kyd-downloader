
from . import GenericParser

def handle_row(row, names, parser=lambda x: x):
    fields = [parser(val.strip()) for val in row.split(';')]
    fields = [None if val == '' else val for val in fields]
    row = dict(zip(names, fields))
    cnpj_fundo_str_num = row['cnpj_fundo'].replace('.', '')\
        .replace('/', '').replace('-', '')
    return (cnpj_fundo_str_num, row)


def handle_informes_diarios(row):
    # CNPJ_FUNDO','DT_COMPTC;VL_TOTAL;VL_QUOTA;VL_PATRIM_LIQ;CAPTC_DIA;RESG_DIA;NR_COTST
    names = ['cnpj_fundo', 'dt_comptc', 'vl_total', 'vl_quota',
             'vl_patrim_liq', 'captc_dia', 'resg_dia', 'nr_cotst']
    parser = GenericParser()
    return handle_row(row, names, parser.parse)


def float_or_none(val):
    try:
        return float(val)
    except:
        return None


def str_or_none(val):
    return str(val) if val else None


def handle_info_cadastral(row):
    # CNPJ_FUNDO;DENOM_SOCIAL;DT_REG;DT_CONST;DT_CANCEL;SIT;DT_INI_SIT;DT_INI_ATIV;DT_INI_EXERC;DT_FIM_EXERC;CLASSE;DT_INI_CLASSE;RENTAB_FUNDO;CONDOM;FUNDO_COTAS;FUNDO_EXCLUSIVO;TRIB_LPRAZO;INVEST_QUALIF;TAXA_PERFM;INF_TAXA_PERFM;TAXA_ADM;INF_TAXA_ADM;VL_PATRIM_LIQ;DT_PATRIM_LIQ;DIRETOR;CNPJ_ADMIN;ADMIN;PF_PJ_GESTOR;CPF_CNPJ_GESTOR;GESTOR;CNPJ_AUDITOR;AUDITOR;CNPJ_CUSTODIANTE;CUSTODIANTE;CNPJ_CONTROLADOR;CONTROLADOR
    names = ['cnpj_fundo', 'denom_social', 'dt_reg', 'dt_const', 'dt_cancel', 
             'sit', 'dt_ini_sit', 'dt_ini_ativ', 'dt_ini_exerc', 'dt_fim_exerc',
             'classe', 'dt_ini_classe', 'rentab_fundo', 'condom', 'fundo_cotas',
             'fundo_exclusivo', 'trib_lprazo', 'invest_qualif', 'taxa_perfm',
             'inf_taxa_perfm', 'taxa_adm', 'inf_taxa_adm', 'vl_patrim_liq',
             'dt_patrim_liq', 'diretor', 'cnpj_admin', 'admin', 'pf_pj_gestor',
             'cpf_cnpj_gestor', 'gestor', 'cnpj_auditor', 'auditor',
             'cnpj_custodiante', 'custodiante', 'cnpj_controlador',
             'controlador']
    vals = handle_row(row, names)
    vals[1]['taxa_perfm'] = float_or_none(vals[1]['taxa_perfm'])
    vals[1]['taxa_adm'] = float_or_none(vals[1]['taxa_adm'])
    vals[1]['vl_patrim_liq'] = float_or_none(vals[1]['vl_patrim_liq'])
    # vals[1]['cnpj_custodiante'] = str_or_none(vals[1]['cnpj_custodiante'])
    # vals[1]['cnpj_controlador'] = str_or_none(vals[1]['cnpj_controlador'])
    # vals[1]['inf_taxa_adm'] = str_or_none(vals[1]['inf_taxa_adm'])
    # vals[1]['inf_taxa_perfm'] = str_or_none(vals[1]['inf_taxa_perfm'])
    return vals

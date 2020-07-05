
import sys
sys.path.append('../kyd-functions/')

import kyd.data.downloaders as dw

import logging

logging.basicConfig(level=logging.INFO)

with open('../config/vna_anbima.json') as fp:
    config = fp.read()
    dw.download_by_config(config, dw.save_file_to_temp_folder)

with open('../config/titpub_anbima.json') as fp:
    config = fp.read()
    dw.download_by_config(config, dw.save_file_to_temp_folder)

with open('../config/taxasswap.json') as fp:
    config = fp.read()
    dw.download_by_config(config, dw.save_file_to_temp_folder)

with open('../config/pricereport.json') as fp:
    config = fp.read()
    dw.download_by_config(config, dw.save_file_to_temp_folder)

with open('../config/cdi.json') as fp:
    config = fp.read()
    dw.download_by_config(config, dw.save_file_to_temp_folder)

with open('../config/cadinstr.json') as fp:
    config = fp.read()
    dw.download_by_config(config, dw.save_file_to_temp_folder)

with open('../config/btc.json') as fp:
    config = fp.read()
    dw.download_by_config(config, dw.save_file_to_temp_folder)

with open('../config/cvm_fundos_info_cadastral.json') as fp:
    config = fp.read()
    dw.download_by_config(config, dw.save_file_to_temp_folder)

with open('../config/cvm_fundos_informe_diario_0.json') as fp:
    config = fp.read()
    dw.download_by_config(config, dw.save_file_to_temp_folder)


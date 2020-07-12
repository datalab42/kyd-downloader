
import sys
sys.path.append('../functions/')

import kyd.data.downloaders as dw

import logging

logging.basicConfig(level=logging.DEBUG)

with open('../config/cvm_fundos_informe_diario_0.json') as fp:
    config = fp.read()
    dw.download_by_config(config, dw.save_file_to_temp_folder)

with open('../config/cvm_fundos_informe_diario_2.json') as fp:
    config = fp.read()
    dw.download_by_config(config, dw.save_file_to_temp_folder)

with open('../config/cvm_fundos_informe_diario_11.json') as fp:
    config = fp.read()
    dw.download_by_config(config, dw.save_file_to_temp_folder)


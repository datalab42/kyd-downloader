
import sys
sys.path.append('../kyd-functions/')

import kyd.data.downloaders as dw

import logging
import datetime

logging.basicConfig(level=logging.DEBUG)

# logging.basicConfig(level=logging.INFO)


dt = datetime.date(2020, 7, 7)

with open('../config/vna_anbima.json') as fp:
    config = fp.read()
    dw.download_by_config(config, dw.save_file_to_temp_folder, refdate=dt)

with open('../config/titpub_anbima.json') as fp:
    config = fp.read()
    dw.download_by_config(config, dw.save_file_to_temp_folder, refdate=dt)

with open('../config/pricereport.json') as fp:
    config = fp.read()
    dw.download_by_config(config, dw.save_file_to_temp_folder, refdate=dt)

with open('../config/cdi.json') as fp:
    config = fp.read()
    dw.download_by_config(config, dw.save_file_to_temp_folder, refdate=dt)

with open('../config/cadinstr.json') as fp:
    config = fp.read()
    dw.download_by_config(config, dw.save_file_to_temp_folder, refdate=dt)

with open('../config/btc.json') as fp:
    config = fp.read()
    dw.download_by_config(config, dw.save_file_to_temp_folder, refdate=dt)

with open('../config/cvm_fundos_info_cadastral.json') as fp:
    config = fp.read()
    dw.download_by_config(config, dw.save_file_to_temp_folder, refdate=dt)

with open('../config/cvm_fundos_informe_diario_0.json') as fp:
    config = fp.read()
    dw.download_by_config(config, dw.save_file_to_temp_folder, refdate=dt)

with open('../config/cotahist.json') as fp:
    config = fp.read()
    dw.download_by_config(config, dw.save_file_to_temp_folder, refdate=dt)


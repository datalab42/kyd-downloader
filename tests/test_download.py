
import sys
sys.path.append('../functions/')

import kyd.data.downloaders as dw
from kyd.data.logs import save_download_logs

import logging
import datetime

logging.basicConfig(level=logging.DEBUG)

# logging.basicConfig(level=logging.INFO)


dt = datetime.date(2020, 8, 2)

# with open('../config/td_ntnb.json') as fp:
#     config = fp.read()
#     res = dw.download_by_config(config, dw.save_file_to_temp_folder, refdate=dt)
#     save_download_logs(res)

# with open('../config/vna_anbima.json') as fp:
#     config = fp.read()
#     res = dw.download_by_config(config, dw.save_file_to_temp_folder, refdate=dt)
#     save_download_logs(res)

with open('../config/titpub_anbima.json') as fp:
    config = fp.read()
    res = dw.download_by_config(config, dw.save_file_to_temp_folder, refdate=dt)
    save_download_logs(res)

# with open('../config/pricereport.json') as fp:
#     config = fp.read()
#     res = dw.download_by_config(config, dw.save_file_to_temp_folder, refdate=dt)
#     save_download_logs(res)

# with open('../config/cdi.json') as fp:
#     config = fp.read()
#     res = dw.download_by_config(config, dw.save_file_to_temp_folder, refdate=dt)
#     save_download_logs(res)

# with open('../config/cadinstr.json') as fp:
#     config = fp.read()
#     res = dw.download_by_config(config, dw.save_file_to_temp_folder, refdate=dt)
#     save_download_logs(res)

# with open('../config/btc.json') as fp:
#     config = fp.read()
#     res = dw.download_by_config(config, dw.save_file_to_temp_folder, refdate=dt)
#     save_download_logs(res)

# with open('../config/cvm_fundos_info_cadastral.json') as fp:
#     config = fp.read()
#     res = dw.download_by_config(config, dw.save_file_to_temp_folder, refdate=dt)
#     save_download_logs(res)

# with open('../config/cvm_fundos_informe_diario_0.json') as fp:
#     config = fp.read()
#     res = dw.download_by_config(config, dw.save_file_to_temp_folder, refdate=dt)
#     save_download_logs(res)

# with open('../config/cotahist.json') as fp:
#     config = fp.read()
#     res = dw.download_by_config(config, dw.save_file_to_temp_folder, refdate=dt)
#     save_download_logs(res)


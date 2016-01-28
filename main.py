# -*- coding: utf-8 -*-
# diretório de exemplo: /badc/cmip5/data/cmip5/output1/BCC/bcc-csm1-1/1pctCO2/mon/ocean/Omon/r1i1p1/latest/tos/*.nc

from ftplib import FTP
import ftplib
import os
from time import strftime
from config import Config

ftp = FTP(Config.host, user=Config.user, passwd=Config.password)

models_file = open('models.txt', 'r')
log_file = open('output.txt', 'a')
log_file.write("\n" + strftime("%Y-%m-%d %H:%M:%S") + "\n")

for models_file_line in models_file.readlines():
    models_file_line = models_file_line.split(" ")
    center = models_file_line[0]
    model = models_file_line[1].replace("\n", "")
    variables_file = open('variables.txt', 'r')

    for linha_variaveis in variables_file.readlines():
        variable_data = linha_variaveis.replace("\n", "").split(" ")
        variable_name = variable_data[0]
        variable_model = variable_data[1]
        variable_table = variable_data[2]
        print("\nDownloading variable %s from %s model %s\n"%(variable_name, center, model))

        for experiment in Config.experiments:
            try:
                directory = Config.directory + '/%s/%s/%s/%s/%s/%s/%s/latest/%s'%(center, model, experiment, Config.frequency, variable_model, variable_table, Config.ensemble, variable_name)
                ftp.cwd(directory)

                file_list = ftp.nlst()

                start_year = int(file_list[0][-16:-12]) - 1
                last_year = start_year + 152

                for nc_file in file_list:
                    if int(nc_file[-16:-12]) >= start_year and int(nc_file[-16:-12]) <= last_year:
                        print("Downloading file %s"%("/".join([directory, nc_file])))

                        ftp.voidcmd('TYPE I')
                        file_size = ftp.size("/".join([directory, nc_file]))

                        try:
                            os.makedirs("/".join(["downloads", center, model, experiment, variable_name]))
                        except OSError:
                            pass

                        local_file = "/".join(["downloads", center, model, experiment, variable_name, nc_file])
                        download = True
                        if os.path.isfile(local_file):
                            print("File already exists, checking...")
                            statinfo = os.stat(local_file)
                            if statinfo.st_size != file_size:
                                print("Remote and local files doesn' match, deleting local file")
                                os.remove(local_file)
                            else:
                                print("Remote and local files match")
                                download = False

                        if download:
                            ftp.retrbinary('RETR %s'%(nc_file), open(local_file, 'wb').write)

            except ftplib.error_perm, resp:
                if "550" in str(resp):
                    log_file.write("Could't find variable %s from %s model %s\n"%(variable_name, center, model))
                    print("Could't find variable %s from %s model %s"%(variable_name, center, model))
                else:
                    raise

    variables_file.close()
    print("Done download files from model %s"%(model))

print("Done downloading files from all models")
models_file.close()
log_file.close()
ftp.quit()
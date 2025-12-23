import numpy as np
import pandas as pd
import xarray as xr 
import datetime
import os, sys
import subprocess


def dl_and_to_nc_data(time, storeDir):
    if (time.date() < datetime.date(2024,2,26)) & (time.minute % 10 != 0):
        print("no data")
        return

    ### output file name
    dayDir = time.strftime("%Y/%m/%d")
    outRainFilePath = "{}/{}/Z__C_RJTD_rain_{}00.nc".format(storeDir, dayDir, time.strftime("%Y%m%d%H%M"))
    outEtopFilePath = "{}/{}/Z__C_RJTD_etop_{}00.nc".format(storeDir, dayDir, time.strftime("%Y%m%d%H%M"))

    ### check file existence
    if os.path.exists(outRainFilePath):
        print("{} Already exists.".format(outRainFilePath))
        return
    else:
        ### if no file exists, download JMA GPV
        mainURL = "http://database.rish.kyoto-u.ac.jp/arch/jmadata/data/jma-radar/synthetic/original"
        os.makedirs("{}/{}".format(storeDir, dayDir), exist_ok=True)

        ### judge input file format
        if time.date() >= datetime.date(2024,2,26):
            ### --------
            ### rain
            ### download
            binFileName = "Z__C_RJTD_{}00_RDR_JMAGPV_Ggis1km_Prr05lv_ANAL_grib2.bin".format(time.strftime("%Y%m%d%H%M"))
            infileURL = "{}/{}/{}".format(mainURL, dayDir, binFileName)
            subprocess.run("wget {} -P {}/{}/".format(infileURL, storeDir, dayDir), shell=True)

            ### binary to netcdf
            rainGgisFile = "{}/{}/{}".format(storeDir, dayDir, binFileName)
            rainNcFilePath = "{}/{}/Z__C_RJTD_{}00_rain.nc".format(storeDir, dayDir, time.strftime("%Y%m%d%H%M"))
            subprocess.run("wgrib2 {} -netcdf {}".format(rainGgisFile, rainNcFilePath), shell=True)
            os.remove(rainGgisFile)

            ### --------
            ### etop
            etopTime = time + datetime.timedelta(minutes=5)
            ### download
            binFileName = "Z__C_RJTD_{}00_RDR_GPV_Ggis1km_Phhlv_Aper5min_ANAL_grib2.bin".format(etopTime.strftime("%Y%m%d%H%M"))
            gzFileName = binFileName + ".gz"
            infileURL = "{}/{}/{}".format(mainURL, dayDir, gzFileName)
            subprocess.run("wget {} -P {}/{}/".format(infileURL, storeDir, dayDir), shell=True)
            subprocess.run("gzip -d {}/{}/{}".format(storeDir, dayDir, gzFileName, storeDir, dayDir), shell=True)

            ### binary to netcdf
            etopGgisFile = "{}/{}/{}".format(storeDir, dayDir, binFileName)
            etopNcFilePath = "{}/{}/Z__C_RJTD_{}00_etop.nc".format(storeDir, dayDir, etopTime.strftime("%Y%m%d%H%M"))
            subprocess.run("wgrib2 {} -netcdf {}".format(etopGgisFile, etopNcFilePath), shell=True)
            os.remove(etopGgisFile)

        else:
            ### download & unzip
            tarFileName = "Z__C_RJTD_{}00_RDR_JMAGPV__grib2.tar".format(time.strftime("%Y%m%d%H%M"))
            infileURL = "{}/{}/{}".format(mainURL, dayDir, tarFileName)
            subprocess.run("wget {} -P {}/{}/".format(infileURL, storeDir, dayDir), shell=True)
            subprocess.run("tar -xvf {}/{}/{} -C {}/{}/".format(storeDir, dayDir, tarFileName, storeDir, dayDir), shell=True)
            os.remove("{}/{}/{}".format(storeDir, dayDir, tarFileName))

            ### binary to netcdf
            rainGgisFile = "{}/{}/Z__C_RJTD_{}00_RDR_JMAGPV_{}_ANAL_grib2.bin".format(storeDir, dayDir, time.strftime("%Y%m%d%H%M"), "Ggis1km_Prr10lv")
            rainNcFilePath = "{}/{}/Z__C_RJTD_{}00_RDR_JMAGPV_{}_ANAL.nc".format(storeDir, dayDir, time.strftime("%Y%m%d%H%M"), "Ggis1km_Prr10lv")
            subprocess.run("wgrib2 {} -netcdf {}".format(rainGgisFile, rainNcFilePath), shell=True)
            os.remove(rainGgisFile)
            etopGgisFile = "{}/{}/Z__C_RJTD_{}00_RDR_JMAGPV_{}_ANAL_grib2.bin".format(storeDir, dayDir, time.strftime("%Y%m%d%H%M"), "Gll2p5km_Phhlv")
            etopNcFilePath = "{}/{}/Z__C_RJTD_{}00_RDR_JMAGPV_{}_ANAL.nc".format(storeDir, dayDir, time.strftime("%Y%m%d%H%M"), "Gll2p5km_Phhlv")
            subprocess.run("wgrib2 {} -netcdf {}".format(etopGgisFile, etopNcFilePath), shell=True)
            os.remove(etopGgisFile)


        rainDA = xr.open_dataset(rainNcFilePath)  
        rainDA = rainDA.rename({"latitude": "lat", "longitude": "lon"})
        rainDA = xr.where(rainDA<1000, rainDA, np.nan)
        rainDA = rainDA.rename_vars({list(rainDA.keys())[0]:"rain"})
        #
        etopDA = xr.open_dataset(etopNcFilePath) 
        etopDA = etopDA.rename({"latitude": "lat", "longitude": "lon"})
        etopDA = xr.where(etopDA<1000, etopDA, np.nan)
        etopDA = etopDA.rename_vars({list(etopDA.keys())[0]:"etop"})

        rainDA.to_netcdf(outRainFilePath)
        etopDA.to_netcdf(outEtopFilePath)
        # outDS = xr.merge([rainDA, etopDA])
        # outDS.to_netcdf(outFilePath)
        os.remove(rainNcFilePath)
        os.remove(etopNcFilePath)

        return


########################################################################
if __name__=="__main__":

    ### download data and convert to netcdf
    ### betweeen "startDatetime" and "endDatetime"
    startDatetime = datetime.datetime(2024,8,27,16,25)
    endDatetime = datetime.datetime(2024,8,27,16,26)

    ### directory to put downloaded data
    storeDir = "./data"



    ###-------------------------
    ### iteration --------------
    sMinute = startDatetime.minute
    startDT10 = startDatetime.replace(minute = sMinute - sMinute % 5, second=0, microsecond=0)
    timeArray = pd.date_range(start=startDT10, end=endDatetime, freq='5min')

    for itemDT in timeArray:
        print(itemDT)
        dl_and_to_nc_data(itemDT, storeDir)

from frastro.frastro.core.data.archive.alerce_archive_cp import AlerceArchive
from frastro.frastro.core.data.archive.lasair_archive_cp import LasairArchive



import json
import os
from pandas.io.json import json_normalize
import pandas as pd
from astropy.table import QTable
from astropy.table import Table, hstack
import numpy as np



class BestCandidates():
    def __init__(self):
        pass



    def searchCadidates(self,days=15):

        alerce = AlerceArchive()
        alerceGoodCandidates = alerce.getCandidates(days)

        lasairbroker = LasairArchive()
        lasairGoodCandidates = lasairbroker.getLastDetections(days)


        # alerceGoodCandidates = [r["result"][target] for target in r["result"]]

        alerceTable = QTable.from_pandas(alerceGoodCandidates)
        lasairTable = QTable.from_pandas(lasairGoodCandidates)


        print("candidates found lasair{0} alerce{1} ".format(len(lasairGoodCandidates),len(alerceGoodCandidates)))
        meanaler_val = {"ramean":np.nan_to_num(np.concatenate((alerceGoodCandidates["meanra"].values, lasairGoodCandidates["meanra"].values), axis=0)),
                        "decmean": np.nan_to_num(np.concatenate((alerceGoodCandidates["meandec"].values, lasairGoodCandidates["meandec"].values), axis=0)),
                        "maggmax": np.nan_to_num(np.concatenate((alerceGoodCandidates["max_magap_g"].values, lasairGoodCandidates["maggmax"].values), axis=0)),
                        "maggmin": np.nan_to_num(np.concatenate((alerceGoodCandidates["min_magap_g"].values, lasairGoodCandidates["maggmin"].values),axis=0)),
                        "magrmax": np.nan_to_num(np.concatenate((alerceGoodCandidates["max_magap_r"].values, lasairGoodCandidates["magrmax"].values),axis=0)),
                        "magrmin": np.nan_to_num(np.concatenate((alerceGoodCandidates["min_magap_r"].values, lasairGoodCandidates["magrmin"].values),axis=0)),
                    "id": np.concatenate((alerceGoodCandidates["oid"].values, lasairGoodCandidates["oid"].values), axis=0)}

        bigdata = pd.DataFrame(meanaler_val)
        bigdata_drop = bigdata.drop_duplicates(subset="id", keep=False)

        bigtable = bigdata
        if bigdata_drop.size > 0:
            bigtable = bigdata_drop



        table_candidates = QTable(QTable.from_pandas(bigtable), masked = False)


        return table_candidates,alerceGoodCandidates,lasairGoodCandidates




if __name__ == "__main__":

    bestCandidates = BestCandidates()
    listOfCandidates = bestCandidates.searchCadidates()

    print(listOfCandidates)


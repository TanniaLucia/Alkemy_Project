import re
import pandas as pd
import logging

def Concat(data1,data2,data3):
    """
    This function concatenate three Dataframe and return this new dataframe
        data1: Corresponds to the first Dataframe
        data2: Corresponds to the second Dataframe
        data3: Corresponds to the third Dataframe
    """
    try:
        data_concat = pd.concat([data1,
                    data2,
                    data3])
        logging.info("The three Dataframes were concatenated successfully")
        return data_concat
    except:
        logging.error("Can't concatenate")

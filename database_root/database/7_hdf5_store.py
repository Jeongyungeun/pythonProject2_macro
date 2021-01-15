import pandas as pd
import h5py


df.to_hdf('database_market.hdf5', index=index, table=True, mode='a')
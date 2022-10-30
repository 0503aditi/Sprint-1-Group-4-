import pandas as pd

chunk_size = 42879
batch_no = 1

for chunk in pd.read_csv('/content/Clean_Dataset.csv' , chunksize=chunk_size, encoding= 'unicode_escape'):
  chunk.to_csv('Airlines' + str(batch_no) + '.csv' , index=False)
  batch_no+=1
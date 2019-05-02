import pandas as pd
import gc
import time
import math
from scipy import spatial
f = open("tams.txt", "w")
def cargar(num):
        nombre_archivo=str(num)+".csv"
        archivo = pd.read_csv(
        nombre_archivo,
        usecols=['userId', 'movieId', 'rating'],
        dtype={'userId': 'int32', 'movieId': 'int32', 'rating': 'float32'})
        matrix = archivo.pivot(
        index='userId', columns='movieId', values='rating').fillna(0)
        del archivo
        #print(len(matrix))
        #print(matrix.index[-1])
        #f.write(str(matrix.index[-1])+",")
        gc.collect()
        return matrix
        
def get_ratings_user(user_id):
        nro_archivo=math.ceil(user_id/8000)
        mi_matrix=cargar(nro_archivo)
        data=mi_matrix.loc[user_id]
        del mi_matrix
        gc.collect()
        return data
def get_distance(array_user,array_files):
        for i in array_files:
                results = np.zeros(8000)
                mi_matrix=cargar(i)
                for i in range(len(mi_matrix.index)):
                        if(userIdRecived!=mi_matrix.index[i]):
                                array_=mi_matrix.iloc[i].values
                                results[i]=1-spatial.distance.cosine(otro,array_)
                        else:
                                results[i]=0 
    #return matrix
seconds = time.time()
print(get_ratings_user(82317))
now_seconds = time.time()
print(now_seconds-seconds)
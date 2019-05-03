import pandas as pd
import gc
import time
import math
from scipy import spatial
import numpy as np
f = open("tams.txt", "w")
MAX_VECINOS=10
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
        data=mi_matrix.loc[user_id].values
        #print(data)
        del mi_matrix
        gc.collect()
        return data
def get_distance(user_id,array_user,array_files):
        #final_list=np.empty(0)
        final_list_valor=[]
        final_list_ids=[]
        for i in array_files:
                if(i==1):
                        results = np.zeros(7999)
                        ids = np.zeros(7999)
                else:
                        results = np.zeros(8000)
                        ids = np.zeros(8000)
                mi_matrix=cargar(i)
                for i in range(len(mi_matrix.index)):
                        if(user_id!=mi_matrix.index[i]):
                                array_=mi_matrix.iloc[i].values
                                tam_1=len(array_)
                                tam_2=len(array_user)
                                if(tam_1!=tam_2):
                                        difference=abs(tam_1-tam_2)
                                        list_n=np.zeros(difference)
                                        #print(type(list_n))
                                        if(tam_1<tam_2):
                                                array_=np.concatenate((array_,list_n))
                                        else:
                                                array_user=np.concatenate((array_user,list_n))
                                #print(len(array_)," vs ",len(array_user))
                                ids[i]=mi_matrix.index[i]
                                #print(ids[i])
                                results[i]=1-spatial.distance.cosine(array_user,array_)
                        else:
                                ids[i]=mi_matrix.index[i]
                                results[i]=0
                ordenada=sorted(range(len(results)), key=lambda k: results[k],reverse=True)
                ordenada=ordenada[:10]
                to_send_valor=[]
                to_send_id=[]
                for i in ordenada:
                        to_send_valor.append(results[i])
                        to_send_id.append(ids[i])
                #print("->>>>",to_send_id)
                #final_list=np.concatenate((final_list,results))
                del results
                del ids
                gc.collect() 
                final_list_valor=final_list_valor+to_send_valor
                final_list_ids=final_list_ids+to_send_id
        return final_list_valor,final_list_ids
def vecinos_cercanos(user_id):
        lista,ids=get_distance(user_id,get_ratings_user(user_id),list(range(1,19)))
        return lista,ids
seconds = time.time()
valores,ids=vecinos_cercanos(82317)
#print(ids)
final_ordenada=sorted(range(len(valores)), key=lambda k: valores[k],reverse=True)
final_ordenada=final_ordenada[0:10]
for i in final_ordenada:
        print("ID: ",int(ids[i])," Valor: ",valores[i])
#print(final_ordenada)
now_seconds = time.time()
print("Tiempo de ejecucion: ",now_seconds-seconds)
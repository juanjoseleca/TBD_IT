import pandas as pd
import gc
import time
import math
from scipy import spatial
import numpy as np
f = open("tams.txt", "w")
MAX_VECINOS=10
NRO_FILES=19
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
                print("nro_documento: ",i," nro_users: ",len(mi_matrix.index))
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
                ordenada=ordenada[:MAX_VECINOS]
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
        valores,ids=get_distance(user_id,get_ratings_user(user_id),list(range(1,NRO_FILES)))
        final_ordenada=sorted(range(len(valores)), key=lambda k: valores[k],reverse=True)
        final_ordenada=final_ordenada[0:MAX_VECINOS]
        nearest_value=[]
        nearest_id=[]
        for i in final_ordenada:
                nearest_id.append(int(ids[i]))
                nearest_value.append(valores[i])
                #print("ID: ",int(ids[i])," Valor: ",valores[i])
        del valores,ids,final_ordenada
        gc.collect()
        return nearest_value,nearest_id
def imprimir_vecinos(valores,ids,k):
        for i in range(k):
                print("ID vecino: ",ids[i]," Valor Coseno: ",valores[i])
def recomendar(user_id,k):
        valores,ids=vecinos_cercanos(user_id)
        valores=valores[:k]
        ids=ids[:k]
        print(valores)
        print(ids)
        total=0
        grado_influencia=[0]*k
        for i in range(k):
                total+=valores[i]
        matrix=[]
        tam_max=0
        for i in range(k):
                array_valores=get_ratings_user(ids[i])
                tam_array=len(array_valores)
                #print(tam_array)
                if(tam_max<tam_array):
                        tam_max=tam_array
                supply=np.zeros(193886-len(array_valores))
                print("1: ",type(supply))
                print("2: ",type(array_valores))
                array_valores=np.concatenate((array_valores,supply))
                #array_valores=array_valores+[0]*(193886-len(array_valores))
                matrix.append(array_valores)
                #proyectado+=array_valores[ids[i]]
                grado_influencia[i]=valores[i]/total
        peliculas_rating=[0]*tam_max
        rating_usuario_principal=get_ratings_user(user_id)
        if(len(rating_usuario_principal)<tam_max):
                rating_usuario_principal+=[0]*tam_max-len(rating_usuario_principal)
        else:
                print("POSIBLE ERROR=================")
        lista=[i for i,x in enumerate(rating_usuario_principal) if x == 0]
        for i in lista:
                proyectado=0
                for j in range(k):
                        proyectado+=matrix[j][i]*grado_influencia[j]
                peliculas_rating[i]=proyectado
        order=sorted(range(len(peliculas_rating)), key=lambda k: peliculas_rating[k],reverse=True)
        order=order[:5]
        for i in order:
                print ("Pelicula: ",i+1," Rating sugerido: ", peliculas_rating[i])





        
seconds = time.time()
valores,ids=vecinos_cercanos(82317)
imprimir_vecinos(valores,ids,10)
#recomendar(82317,3)
#print(ids)
#print(len(get_ratings_user(42344)))

#print(final_ordenada)
now_seconds = time.time()
print("Tiempo de ejecucion: ",now_seconds-seconds)
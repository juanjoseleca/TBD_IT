import pandas as pd
import gc
import time
import math
from scipy import spatial
import numpy as np


import pymp #OPENMP
from mpi4py import MPI #MPI

comm = MPI.COMM_WORLD 
rank = comm.Get_rank() 

MAX_VECINOS=10
NUM_OF_FILES = 6

NUM_OF_PC =2
NUM_OF_PROCESS = 4
files_per_pc=6

idUsuario =16006


seconds = time.time()


def cargar(num):
        nombre_archivo="/home/mpiuser/dataset/"+str(num)+".csv"
        archivo = pd.read_csv(
        nombre_archivo,
        usecols=['userId', 'movieId', 'rating'],
        dtype={'userId': 'int32', 'movieId': 'int32', 'rating': 'float32'})
        matrix = archivo.pivot(
        index='userId', columns='movieId', values='rating').fillna(0)
        del archivo
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
                
                #cargando matriz
                mi_matrix=cargar(i)
                
                cant_users=len(mi_matrix.index)
                
                results = np.zeros(cant_users)
                ids = np.zeros(cant_users)
                
                with pymp.Parallel(8) as p:
                        len_matrix=len(mi_matrix.index)
                        for i in p.range(len_matrix):

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
                                                del list_n
                                        #print(len(array_)," vs ",len(array_user))
                                        ids[i]=mi_matrix.index[i]
                                        #print(ids[i])
                                        #COSENO
                                        results[i]=1-spatial.distance.cosine(array_user,array_)
                                        #EUCLINIAN
                                        #results[i]=spatial.distance.euclidean(array_user,array_)
                                        #results[i]=np.linalg(array_user,array_)
                                        #PEARSON
                                        #results[i]=spatial.distance.correlation(array_user,array_)
                                        #results[i]=spatial.distance.corrcoef(array_user,array_)[0,1]
                                        
                                else:
                                        ids[i]=mi_matrix.index[i]
                                        results[i]=0
                        
                del mi_matrix

                ordenada=sorted(range(len(results)), key=lambda k: results[k],reverse=True)
                
                ordenada=ordenada[:MAX_VECINOS]
                to_send_valor=[]
                to_send_id=[]
                for i in ordenada:
                        to_send_valor.append(results[i])
                        to_send_id.append(ids[i])
                
                del results
                del ids
                

                gc.collect() 
                final_list_valor=final_list_valor+to_send_valor
                final_list_ids=final_list_ids+to_send_id
                
        return final_list_valor,final_list_ids



def vecinos_cercanos(ratings_user,user_id,beginFile,endFile):
        numFiles = list(range(beginFile,endFile))

        lista,ids = get_distance(user_id,ratings_user, numFiles )
        
        final_ordenada=sorted(range(len(lista)), key=lambda k: lista[k],reverse=True)
        final_ordenada=final_ordenada[0:10]
        
        result_scores=np.zeros(MAX_VECINOS)
        result_ids=np.zeros(MAX_VECINOS)
        
        it =0
        for i in final_ordenada:
                result_scores[it] =lista[i]
                result_ids[it] = int(ids[i])
                it+=1
        del lista
        del ids 
        del final_ordenada

        return result_scores,result_ids

def recomendar(user_id,valores,ids):
        k=MAX_VECINOS
        
        valores=valores[:k]
        ids=ids[:k]
        
        total=0
        grado_influencia=[0]*k
        for i in range(k):
                total+=valores[i]
        matrix=[]
        tam_max=0
        for i in range(k):
                array_valores=get_ratings_user(ids[i])
                #print(array_valores)
                tam_array=len(array_valores)
                #print(tam_array)
                if(tam_max<tam_array):
                        tam_max=tam_array
                supply=np.zeros(193886-len(array_valores))
                #print("1: ",type(supply))
                #print("2: ",type(array_valores))
                array_valores=np.concatenate((array_valores,supply))
                #array_valores=array_valores+[0]*(193886-len(array_valores))
                matrix.append(array_valores)
                del array_valores
                gc.collect()
                #proyectado+=array_valores[ids[i]]
                grado_influencia[i]=valores[i]/total
        #print(matrix)
        peliculas_rating=[0]*193886
        rating_usuario_principal=get_ratings_user(user_id)  
        a_supply=np.zeros(193886-len(rating_usuario_principal))
        rating_usuario_principal=np.concatenate((rating_usuario_principal,a_supply))   
        #print("POSIBLE ERROR=================")
        lista=[i for i,x in enumerate(rating_usuario_principal) if x == 0]
        
        for i in lista:
                proyectado=0
                for j in range(k):
                        proyectado+=matrix[j][i]*grado_influencia[j]
                peliculas_rating[i]=proyectado
        
        order=sorted(range(len(peliculas_rating)), key=lambda k: peliculas_rating[k],reverse=True)
        order=order[:10]
        df_movies = pd.read_csv(
            "/home/mpiuser/dataset/movies.csv",
            usecols=['movieId', 'title'],
            dtype={'movieId': 'int32', 'title': 'str'})
        print(df_movies)
        for i in order:
                print ("Pelicula: ",df_movies.at[i,'title']," Rating sugerido: ", peliculas_rating[i])


#---------------------------------------------

print(rank)


ratings_user_query = get_ratings_user(idUsuario)

parcial_res =[]
parcial_dni =[]

if rank == 0:
        print("iniciando")
        print("paso 1")
	
        len_ratings_user_query = len(ratings_user_query)
        it_files=0
        
        ratings_user_query = np.copy(ratings_user_query.T[0])
        
        for i in range(1,NUM_OF_PC):
                
                parameters=np.zeros(4,dtype="int32")                
                parameters[0]=idUsuario
                parameters[1]=it_files+1
                parameters[2]=it_files+files_per_pc
                parameters[3]=len_ratings_user_query
                
                it_files=it_files+files_per_pc

                comm.Send(parameters, dest=i)   
                comm.Send(ratings_user_query, dest=i)
                
        array_all_results = np.zeros(MAX_VECINOS*NUM_OF_PC-1)

        del ratings_user_query
                       
else :
        parameters=np.zeros(4,dtype="int32")
        comm.Recv(parameters, source=0) 
        
        ratings_user= np.zeros(parameters[3],dtype="float32")
        comm.Recv(ratings_user, source=0) 

        id_user = parameters[0]
        it_begin = parameters[1]
        it_end = parameters[2]
        
        parcial_res, parcial_dni = vecinos_cercanos(ratings_user_query,idUsuario,it_begin,it_end)
        
        
        del ratings_user_query

#---------------------------------------------


parcialRes=comm.gather( parcial_res ,root=0)
parcialDNI=comm.gather( parcial_dni ,root=0)

DISTANCES = np.zeros(0)
IDS = np.zeros(0)

for i in range(1,NUM_OF_PC): 
        DISTANCES=np.append(DISTANCES,parcialRes[i])       
        
for i in range(1,NUM_OF_PC): 
        IDS=np.append(IDS,parcialDNI[i])       

i_sorted=sorted(range(len(DISTANCES)), key=lambda k: DISTANCES[k],reverse=False)
s_distance=[0]*len(DISTANCES)
s_ids=[0]*len(DISTANCES)
for i in range(len(i_sorted)):
        s_distance[i]=DISTANCES[i_sorted[i]]
        s_ids[i]=IDS[i_sorted[i]]


print(idUsuario)
print(s_distance)
print(s_ids)

#recomendar(idUsuario,s_distance,s_ids)

now_seconds = time.time()

print("Tiempo de ejecucion: ",now_seconds-seconds)


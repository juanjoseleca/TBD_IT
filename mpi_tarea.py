from mpi4py import MPI
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size=comm.size
nro_usuarios=283228
nro_peliculas=193886
print("nro_procesos:",size)
def recomendar(id,nro_usuarios=283228,procesos=size):
    lista=np.empty((0,2),int)
    for i in range(1,id):
        #print(i)
        lista=np.append(lista,[[i,id]],axis=0)
    for i in range(id+1,nro_usuarios+1):
        #print(i)
        lista=np.append(lista,[[i,id]],axis=0)
    print(lista)
    #print(np.split(lista,procesos))
    
def obtener_pesos(array,id_usuario):
    for i in array:
        print("here")

if (rank == 0):
    recomendar(5)
    data = {'a': 7, 'b': 3.14}
    data['a']=5
    data['b']=7
    comm.send(data, dest=1)
elif (rank == 1):
    data = comm.recv(source=0)
    print(data)
#comm.Barrier()
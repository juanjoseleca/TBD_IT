import pandas as pd
import math
from csv_splitter import split
def Coseno(x,y):
    if(len(x)!=len(y)):
        print("error de tamaño")
        return
    sum1=0
    sum2=0
    sum3=0
    n=len(x)
    for i in range(n):
        sum1+=x[i]**2
        sum2+=y[i]**2
        sum3+=x[i]*y[i]
    return sum3/(math.sqrt(sum1)*math.sqrt(sum2))
class RecommenderSystem:
    def __init__(self,location):
        self.location=location
        

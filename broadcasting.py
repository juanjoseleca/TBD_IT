from mpi4py import MPI 
from scipy import spatial
import os
import time
import gc
import argparse
import numpy as np
import math
# data science imports
import pandas as pd
from scipy.sparse import csr_matrix
from sklearn.neighbors import NearestNeighbors

# utils import
from fuzzywuzzy import fuzz


comm = MPI.COMM_WORLD 
rank = comm.Get_rank() 

#https://stackabuse.com/creating-a-simple-recommender-system-in-python-using-pandas/

#977222917

size = comm.Get_size()

print size , "............................."


def Coseno(x,y):
    if(len(x)!=len(y)):
        print "error de tamano"
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

class KnnRecommender:
    def __init__(self, path_movies, path_ratings):
        self.path_movies = path_movies
        self.path_ratings = path_ratings
        self.movie_rating_thres = 0
        self.user_rating_thres = 0
        self.model = NearestNeighbors()
	self.sizeFileRatings = 0

    def set_filter_params(self, movie_rating_thres, user_rating_thres):
        self.movie_rating_thres = movie_rating_thres
        self.user_rating_thres = user_rating_thres

    def set_model_params(self, n_neighbors, algorithm, metric):
        self.model.set_params(**{
            'n_neighbors': n_neighbors,
            'algorithm': algorithm,
            'metric': metric,})

    def _prep_data(self,iFileRatings):
        df_movies = pd.read_csv(
            "/home/mpiuser/cloud/DB/movies.csv",
            usecols=['movieId', 'title'],
            dtype={'movieId': 'int32', 'title': 'str'})
        df_ratings = pd.read_csv(
            iFileRatings,
            usecols=['userId', 'movieId', 'rating'],
            dtype={'userId': 'int32', 'movieId': 'int32', 'rating': 'float32'})
        df_movies_cnt = pd.DataFrame(
            df_ratings.groupby('movieId').size(),
            columns=['count'])
        popular_movies = list(set(df_movies_cnt.query('count >= @self.movie_rating_thres').index))  # noqa
        movies_filter = df_ratings.movieId.isin(popular_movies).values

        df_users_cnt = pd.DataFrame(
            df_ratings.groupby('userId').size(),
            columns=['count'])
        active_users = list(set(df_users_cnt.query('count >= @self.user_rating_thres').index))  # noqa
        users_filter = df_ratings.userId.isin(active_users).values
        df_ratings_filtered = df_ratings[movies_filter & users_filter]
        movie_user_mat = df_ratings_filtered.pivot(
            index='userId', columns='movieId', values='rating').fillna(0)

        print(movie_user_mat)
        hashmap = {
            movie: i for i, movie in
            enumerate(list(df_movies.set_index('movieId').loc[movie_user_mat.index].title)) # noqa
        }

        del df_movies, df_movies_cnt, df_users_cnt
        del df_ratings, df_ratings_filtered
        gc.collect()

        return movie_user_mat, hashmap


    def _fuzzy_matching(self, hashmap, fav_movie):
        match_tuple = []
        # get match
        for title, idx in hashmap.items():
            ratio = fuzz.ratio(title.lower(), fav_movie.lower())
            if ratio >= 60:
                match_tuple.append((title, idx, ratio))
        # sort
        match_tuple = sorted(match_tuple, key=lambda x: x[2])[::-1]
        if not match_tuple:
            print('Oops! No match is found')
        else:
            print('Found possible matches in our database: '
                  '{0}\n'.format([x[0] for x in match_tuple]))
            return match_tuple[0][1]

    def _inference(self, model, data, hashmap,
                   fav_movie, n_recommendations):
        # fit
        model.fit(data)

        print('You have input movie:', fav_movie)
        idx = self._fuzzy_matching(hashmap, fav_movie)
        print("-------->",idx,fav_movie)

        print('Recommendation system start to make inference')
        print('......\n')
        t0 = time.time()
        distances, indices = model.kneighbors(
            data[idx],
            n_neighbors=n_recommendations+1)

        raw_recommends = \
            sorted(
                list(
                    zip(
                        indices.squeeze().tolist(),
                        distances.squeeze().tolist()
                    )
                ),
                key=lambda x: x[1]
            )[:0:-1]
        print('It took my system {:.2f}s to make inference \n\
              '.format(time.time() - t0))
        # return recommendation (movieId, distance)
        return raw_recommends

    def make_recommendations(self, fav_movie, n_recommendations,iFileRatings):
        # get data
        movie_user_mat, hashmap = self._prep_data(iFileRatings)
	return movie_user_mat
	
        # get recommendations
	
		
		
	print "hello", rank 
		
	"""
	raw_recommends = self._inference(
            self.model, movie_user_mat_sparse, hashmap,
            fav_movie, n_recommendations)
        # print results
        reverse_hashmap = {v: k for k, v in hashmap.items()}
        print('Recommendations for {}:'.format(fav_movie))
        for i, (idx, dist) in enumerate(raw_recommends):
            print('{0}: {1}, with distance '
                  'of {2}'.format(i+1, reverse_hashmap[idx], dist))
	"""

if rank == 0:
	filteredUser=[]
	userId=123
	if userId < 25488 :
		rating_user = pd.read_csv("/home/mpiuser/cloud/DB/by12/1.csv",usecols=['userId', 'movieId', 'rating'],dtype={'userId': 'int32', 'movieId': 'int32', 'rating': 'float32'})
	
		filteredUser = rating_user[(rating_user['userId'] == userId)]
		
		filteredUser = filteredUser.values.transpose()[1:3]
		print filteredUser 
	
	else:
		rating_user = pd.read_csv("/home/mpiuser/cloud/DB/by12/2.csv",usecols=['userId', 'movieId', 'rating'],dtype={'userId': 'int32', 'movieId': 'int32', 'rating': 'float32'})
	sizeBuffer = np.zeros(1,dtype="int32")
	idUser_to_send = np.zeros(1,dtype="int32")	
	
	sizeBuffer[0]=len(filteredUser[0])
	idUser_to_send[0]=userId
		
	comm.Send(sizeBuffer, dest=1)
	comm.Send(idUser_to_send, dest=1)
	comm.Send(filteredUser, dest=1)
	for i in range(1,size):
		comm.Send(sizeBuffer, dest=i)
		comm.Send(idUser_to_send, dest=i)
		comm.Send(filteredUser, dest=i)

else :

			
    sizeBuffer=np.zeros(1,dtype="int32")
    userIdRecived = np.zeros(1,dtype="int32")

    comm.Recv(sizeBuffer, source=0) 
    comm.Recv(userIdRecived, source=0) 
    sizeBuffer=np.int32(sizeBuffer[0])

    filteredUser=np.zeros((2,sizeBuffer)).transpose()

    comm.Recv(filteredUser, source=0)

    userID=userIdRecived[0]
    userRatings =filteredUser
    print "...................................."
    recommender = KnnRecommender("/home/mpiuser/cloud","/home/mpiuser/cloud/by12")
    recommender.set_filter_params(0, 20)
    recommender.set_model_params(20, 'brute', 'cosine')
    mi_matrix=recommender.make_recommendations(userIdRecived[0], 5,"/home/mpiuser/cloud/DB/by12/"+str(rank)+".csv")
    #for i in range(len(mi_matrix.index)):
        
    #print type(mi_matrix.iloc[2])
    otro=[0]*27177
    print "======="
    print filteredUser
    for i in filteredUser:
        otro[int(i[0])]=np.float(i[1])
    #print "otro: ",otro
    #print "------->",mi_matrix.shape
    
    results = np.zeros(9794)

    for i in range(len(mi_matrix.index)):
        if(userIdRecived!=mi_matrix.index[i]):
            array_=mi_matrix.iloc[i].values
            results[i]=1-spatial.distance.cosine(otro,array_)
        else:
            results[i]=0
    
    print mi_matrix.index

    #results=np.sort(results)


    """
    rating_user = pd.read_csv("/home/mpiuser/cloud/DB/by12/1.csv",usecols=['userId', 'movieId', 'rating'],dtype={'userId': 'int32', 'movieId': 'int32', 'rating': 'float32'})
    
    for i in results:
        filteredUser = rating_user[(rating_user['userId'] == userId)]
	"""	


    #print "EL RESULTADO :'V ", results
    

    
    #for i in mi_matrix:
    #	print "->", i

 
import sys
import itertools
from scipy import spatial
from itertools import combinations
import numpy 
import math
import itertools
from mrjob.job import MRJob
from mrjob.step import MRStep



class RecMovies(MRJob):



    def configure_options(self): 
        
        super(RecMovies, self).configure_options()

        self.add_passthrough_option('-m','--movie', action="append", type='str', default=[], help='Name of Movies')
     
        self.add_passthrough_option('-k','--no-of-items', type='int',default=25, help='No of similar movies')

        self.add_passthrough_option('-l','--lwr-similarity', type='float',default=0.4, help='Lowerbound similarity')

        self.add_passthrough_option('-p','--minimum-ratingPairs', type='int', default=3, help='Minimum rating pairs needed by movie' + 'to be similar')

                   

        
    def steps(self):
        
        return [
            MRStep(mapper=self.mapperReadFiles, reducer=self.reducerMovieNameRating),
            MRStep(mapper=self.mapperUserMovRatingPair, reducer=self.reducerUserMovRatingPair),
            MRStep(reducer=self.reducerMovKeyRatingVal),
            MRStep(reducer=self.reducerRecMovies),
            MRStep(reducer=self.reducerSortedRecMovies)
        ]




    def mapperReadFiles(self, _,line):

        words=line.split('::')
        if(len(words) < 4):
            yield (words[0],words[1])
        else:
            values=words[:]
            del values[3]
            yield (words[1],values) 




    def reducerMovieNameRating(self, key, values):
        
        values=list(values)
        for value in values:
            if isinstance(value,basestring):
                movie="".join(filter(lambda x: ord(x)<128, value))
        for value in values:
            if not isinstance(value,basestring):
                yield "{}::{}::{}::{}".format(value[0],value[1],movie,value[2]),""




    def mapperUserMovRatingPair(self, line,_):
        
        words=line.split('::')
        yield (words[0], (words[2],words[3]))



    def reducerUserMovRatingPair(self, key, values):
        
        values=list(values)
        for i,j in combinations(sorted(values),2)
            if(i[0]<j[0]): yield (i[0],j[0]),(i[1],j[1]) 
            else:  yield (j[0],i[0]),(j[1],i[1])




    def reducerMovKeyRatingVal(self,key,values):
        
        rating_list=list(values)
        if len(rating_list) >= self.options.minimum_ratingPairs:   
            rating1,rating2=zip(*(rating_list)) 
            rating1=map(int,rating1)
            rating2=map(int,rating2)
            corr = numpy.corrcoef(rating1,rating2)[0,1]
            cosineCorr = 1 - spatial.distance.cosine(rating1,rating2)
            averageCorr = 0.5 * ( corr + cosineCorr)
            if((not math.isnan(averageCorr))
                and (avgageCorr >= self.options.lwr_similarity) ):
                yield key,(avegareCorr,int(numpy.mean(rating1)),int(numpy.mean(rating2)))




    def reducerRecMovies(self,key,values):
        
        value=list(values)[0]

        for movie in self.options.movie:
            if(movie == key[0]): yield movie, (key[1],value[0],value[2])
            elif(movie == key[1]): yield movie,(key[0],value[0],value[1])



    def reducerSortedRecMovies(self,key,values):
        
        outputDict=dict()

        for value in values: outputDict[value[1]]=value
        for val in sorted(outputDict, reverse = True)[:self.options.no_of_items]: print(key,outputDict[val])





if __name__ == '__main__': RecMovies.run()

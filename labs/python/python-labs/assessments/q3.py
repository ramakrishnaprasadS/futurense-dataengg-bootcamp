
from mrjob.job import MRJob

class MoviesCount(MRJob):
    def mapper(self,_,record):
        yoR = record.split(",")[-1]
        # yield(yoR,1) if yoR.isnumeric() else yoR
        if yoR.isnumeric():
            yield(int(yoR),1)
       

    def reducer(self,key,values):
        yield(key,sum(values))


if __name__ == '__main__':
    MoviesCount.run()



# from mrjob.job import MRJob

# class yearbyMovies(MRJob):

#     def mapper(self, key, line):
#         l = line.split(',')
#         y = l[-1]

#         if y.isnumeric():
#             yield(y,1) 
#         else:
#             pass
        
#     def reducer(self,key, count):
#             yield(key,sum(count))

# if __name__ == '__main__':
#     yearbyMovies.run()
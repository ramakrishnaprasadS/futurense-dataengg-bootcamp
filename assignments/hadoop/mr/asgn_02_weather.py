
from mrjob.job import MRJob

class MaxTemp(MRJob):

    def mapper(self,sno,record):
        yield("maxtemp",record.split()[5])
        yield("mintemp",record.split()[6])

    def reducer(self,key,values):
        if key=="maxtemp":
            yield("maxtemp",max(values))
        if key=="mintemp":
            yield("mintemp",min(values))


if __name__ == '__main__':
    MaxTemp.run()

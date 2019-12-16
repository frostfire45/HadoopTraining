from mrjob.job import MRJob
from mrjob.step import MRStep

class MovieCountUp(MRJob):
    #This is what defines the Map and reducer
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings, reducer=self.reducer_count_ratings)
        ]
    # yields is return, this example just returns the movie and how many times it was display
    def mapper_get_ratings(self, _, line):
        (userID, movieID, rating, timestamp) = line.split ('\t')
        yield movieID, 1

    def reducer_count_ratings(self, key, values):
        yield key, sum(values)
if __name__ == '__main__':
    MovieCountUp.run()

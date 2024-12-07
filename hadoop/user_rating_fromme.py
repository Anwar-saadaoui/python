from mrjob.job import MRJob
from mrjob.step import MRStep


class CountRating(MRJob):
    
    def steps(self):
        return [
            MRStep(
                mapper= self.mapper_most_rating,
                reducer = self.reducer_most_rating
            )
        ]

    def mapper_most_rating(self,_, line):
        (userID, movieID, rating, timestamp) = line.split("\t")
        yield movieID, int(rating)
    
    def reducer_most_rating(self,key, values):
        yield key , sum(values)
    

if __name__ == '__main__':
    CountRating.run()

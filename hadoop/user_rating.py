from mrjob.job import MRJob
from mrjob.step import MRStep


class UserRating(MRJob):

    def steps(self):
        return [
            MRStep(
                mapper = self.mapper_get_user_rating,
                reducer = self.reducer_get_user_rating
                )
        ]

    def mapper_get_user_rating(self,_, line):
        (userID, movieID, rating, timestamp) = line.split("\t") 
        yield movieID, 1

    def reducer_get_user_rating(self,key ,values):
        yield key, sum(values)

if __name__ == '__main__':
    UserRating.run()
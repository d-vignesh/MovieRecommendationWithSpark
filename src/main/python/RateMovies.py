import os
from os import remove, removedirs
from os.path import dirname, join, isfile
from time import time

topMovies = """1, Toy Story (1995)
780, Independence Day (a.k.a ID4) (1996)
590,Dances with Wolves (1990)
1210,Star Wars: Episode VI - Return of the Jedi (1983)
648,Mission: Impossible (1996)
344,Ace Ventura: Pet Detective (1994)
165,Die Hard: With a Vengeance (1995)
153,Batman Forever (1995)
597,Pretty Woman (1990)
1580,Men in Black (1997)
231,Dumb & Dumber (1994)"""

parentDir = os.path.dirname(os.path.realpath(__file__))
personalRatingsFile = join(parentDir, "personalRatings.csv")
# print(personalRatingsFile)

if isfile(personalRatingsFile):
	r = input("looks like you've already rated the movies. Overwrite ratings (y/N)?")
	if r and r[0].lower() == "y":
		remove(personalRatingsFile)
	else:
		sys.exit()

prompt = "please rate the following movies (1-5 (best), or 0 if not seen): "
print(prompt)

now = int(time())
n = 0

with open(personalRatingsFile, "w") as file:
	file.write("userId,movieId,rating,timestamp\n")
	for line in topMovies.split("\n"):
		ls = line.strip().split(",")
		valid = False
		while not valid:
			rating = input(ls[1] + " : ")
			rating = int(rating) if rating.isdigit() else -1
			if rating < 0 or rating > 5:
				print(prompt)
			else:
				valid = True
				file.write("1,%s,%d,%d\n" % (ls[0], rating, now))
				n += 1

if n == 0:
	print("no rating provided..")
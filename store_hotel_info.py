import csv
import numpy as np
from scipy.stats import skew

inputFile = open('Hotel_Reviews_clean.csv', 'r')
reader = csv.DictReader(inputFile)

reviews = list(reader)

inputFile.close()

processedHotels = {}

# Get the addresses and cities for all of the hotels
for review in reviews:
    if review['hotel_name'] not in processedHotels:
        processedHotels[review['hotel_name']] = { "address": review["hotel_address"], "city": review["city"], "latitude": review["hotel_latitude"], "longitude": review["hotel_longitude"] }

print("Got all hotels")

# Get average and standard deviation of scores for each hotel_latitude
i = 1

fieldNames = ["name", "address", "city", "latitude", "longitude", "review_count", "average_score", "score_standard_deviation", "skew"]
outputFile = open('hotel_info.csv', 'w')
writer = csv.DictWriter(outputFile, fieldnames=fieldNames)
writer.writeheader()

for hotel in processedHotels:
    print(str(i) + ". " + hotel)
    relevantReviews = filter(lambda x: x["hotel_name"] == hotel, reviews)
    hotelScores = np.array(map(lambda x: float(x["score"]), relevantReviews))
    averageScore = hotelScores.mean()
    standardDeviation = hotelScores.std()
    processedHotels[hotel]["name"] = hotel
    processedHotels[hotel]["average_score"] = averageScore
    processedHotels[hotel]["score_standard_deviation"] = standardDeviation
    processedHotels[hotel]["review_count"] = hotelScores.size
    processedHotels[hotel]["skew"] = skew(hotelScores)
    writer.writerow(processedHotels[hotel])
    i += 1

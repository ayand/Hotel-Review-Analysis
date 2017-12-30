import csv

inputFile = open('Hotel_Reviews.csv', 'r')
outputFile = open('Hotel_Reviews_clean.csv', 'w')

numberOfInputs = 0
numberOfOutputs = 0

fieldnames = ['hotel_address', 'hotel_name', 'reviewer_origin', 'number_user_reviews', 'score', 'hotel_latitude', 'hotel_longitude', 'review_text', 'sentiment', 'visit_length', 'trip_type', 'visitor_type', 'city']

reader = csv.DictReader(inputFile)
writer = csv.DictWriter(outputFile, fieldnames=fieldnames)
writer.writeheader()

distinctTags = set()

coupleCount = 0
olderChildrenCount = 0
youngChildrenCount = 0
groupCount = 0
soloTravelerCount = 0
travelersFriendsCount = 0

for row in reader:
    numberOfInputs += 1
    newRow = {}
    newRow['hotel_address'] = row["Hotel_Address"]
    newRow['hotel_name'] = row["Hotel_Name"]
    newRow['reviewer_origin'] = row["Reviewer_Nationality"]
    newRow['number_user_reviews'] = row["Total_Number_of_Reviews_Reviewer_Has_Given"]
    newRow['score'] = row["Reviewer_Score"]
    newRow['hotel_latitude'] = row["lat"]
    newRow['hotel_longitude'] = row["lng"]
    positiveText = None
    negativeText = None
    if row["Negative_Review"] == "No Negative":
        negativeText = ""
    else:
        negativeText = row["Negative_Review"]
    if row["Positive_Review"] == "No Positive":
        positiveText = ""
    else:
        positiveText = row["Positive_Review"]
    if positiveText + negativeText != "":
        newRow['review_text'] = (positiveText + negativeText)
        if float(newRow['score']) >= 6.0:
            #print('positive')
            newRow['sentiment'] = 1
        else:
            #print('negative')
            newRow['sentiment'] = 0
        tags = row["Tags"].replace("[","").replace("]", "").replace("'", "")
        tagsSplit = tags.split(",")
        for tag in tagsSplit:
            distinctTags.add(tag.strip())
            if "Stayed" in tag:
                #distinctTags.add(tag.strip())
                components = tag.strip().split(" ")
                days = components[1]
                daysNum = int(days)
                newRow["visit_length"] = daysNum
            elif "visit_length" not in newRow:
                newRow["visit_length"] = ""
            if "Trip" in tag or "trip" in tag:
                if tag.strip() in ["Business trip", "Leisure trip"]:
                    #print("Found trip type")
                    tripType = tag.strip()
                    if tripType == "Business trip":
                        newRow["trip_type"] = "Business"
                    else:
                        newRow["trip_type"] = "Leisure"
                elif "trip_type" not in newRow:
                    newRow["trip_type"] = ""
            elif "trip_type" not in newRow:
                newRow["trip_type"] = ""
            if tag.strip() in ["Couple", "Family with older children", "Family with young children", "Group", "Solo traveler", "Travelers with friends"]:
                #print(tag.split())
                partyType = tag.strip()
                newRow['visitor_type'] = partyType
                if partyType == "Couple":
                    coupleCount += 1
                elif partyType == "Family with older children":
                    olderChildrenCount += 1
                elif partyType == "Family with young children":
                    youngChildrenCount += 1
                elif partyType == "Group":
                    groupCount += 1
                elif partyType == "Solo traveler":
                    soloTravelerCount += 1
                elif partyType == "Travelers with friends":
                    travelersFriendsCount += 1
            elif "visitor_type" not in newRow:
                newRow['visitor_type'] = ""
        if "Amsterdam" in row["Hotel_Address"]:
            newRow['city'] = "Amsterdam"
        elif "London" in row["Hotel_Address"]:
            newRow['city'] = "London"
        elif "Barcelona" in row["Hotel_Address"]:
            newRow['city'] = "Barcelona"
        elif "Vienna" in row["Hotel_Address"]:
            newRow['city'] = "Vienna"
        elif "Milan" in row["Hotel_Address"]:
            newRow['city'] = "Milan"
        else:
            newRow['city'] = "Paris"
        writer.writerow(newRow)
        numberOfOutputs += 1

outputFile.close()
inputFile.close()

print('Number of inputs: ' + str(numberOfInputs))
print('Number of outputs: ' + str(numberOfOutputs))

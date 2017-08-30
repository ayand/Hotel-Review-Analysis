import csv

inputFile = open('Hotel_Reviews.csv', 'r')
outputFile = open('Hotel_Reviews_clean.csv', 'w')

numberOfInputs = 0
numberOfOutputs = 0

fieldnames = ['hotel_address', 'hotel_name', 'reviewer_origin', 'number_user_reviews', 'score', 'hotel_latitude', 'hotel_longitude', 'review_text', 'sentiment']

reader = csv.DictReader(inputFile)
writer = csv.DictWriter(outputFile, fieldnames=fieldnames)
writer.writeheader()

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
        if float(newRow['score']) > 5.0:
            print('positive')
            newRow['sentiment'] = 1
        else:
            print('negative')
            newRow['sentiment'] = 0
        writer.writerow(newRow)
        numberOfOutputs += 1

        print('Wrote new row')

print('Done')
outputFile.close()
inputFile.close()

print('Number of inputs: ' + str(numberOfInputs))
print('Number of outputs: ' + str(numberOfOutputs))

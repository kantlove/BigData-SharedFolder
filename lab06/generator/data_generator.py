import random
import os

# ==============================================
# Helpers
# ==============================================
def writeToFileAppend(fileName, list):
    out = open(fileName, 'a')

    for i in xrange(0, len(list)):
        item = list[i]
        write_str = beautify(str(item))
        if i == 0:
            out.write(write_str + "\t")
        else:
            out.write(write_str + " ")

    out.write("\n")
    out.close();

def writeToFile(fileName, list):
    out = open(fileName, 'w')

    for item in list:
        write_str = beautify(str(item))
        out.write(write_str + "\n")

    out.close();

def beautify(text):
    return text.replace('[','').replace(']','').replace(',','')

def randCoor():
    return random.randint(min_coordinate, max_coordinate)

def indexOf(list, item):
    try:
        id = list.index(item)
        return id
    except ValueError:
        return -1

def createPoint(dim):
    result = []
    for i in xrange(0, dim):
        result.append(randCoor())
    return result

# ==============================================
# Main
# ==============================================
limit = 1000
n_centers = 30
dimensions = 2
max_coordinate = 100 
min_coordinate = 0
center_file = "centers.txt"
dataset_file = "dataset.txt"
input_file = "kmeans_input.txt"

# Create dataset of points
print "Creating dataset"

points = []
for i in xrange(0, limit):
    points.append(createPoint(dimensions))

writeToFile(dataset_file, points)

# Pick randomly some centers
print "Choosing centers"

center_ids = []
range_to_pick = range(0, limit)
for i in xrange(0, n_centers):
    chosen = random.randint(0, len(range_to_pick) - 1)
    center_ids.append(range_to_pick[chosen])
    
    range_to_pick.pop(chosen)

centers = []
for i in center_ids:
    centers.append(points[i])
writeToFile(center_file, centers)

# Delete old file
os.remove("kmeans_input.txt")

# Arrange elements to be children of a center randomly
print "Choosing children"

remainings = []
for i in xrange(0, len(points)):
    if indexOf(center_ids, i) < 0: # ignore centers
        remainings.append(i) # only save the index

for i in xrange(0, len(centers)):
    c = centers[i]
    record = [c, points.index(c)]

    if i == len(centers) - 1:
        size = len(remainings)
    else:
        size = random.randint(0, len(remainings))

    for i in xrange(0, size):
        chosen = random.randint(0, len(remainings) - 1)
        record.append(remainings[chosen])
        remainings.pop(chosen)
    writeToFileAppend(input_file, record)


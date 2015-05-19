from datetime import datetime

# file = open("/Users/WofloW/project/test/file_1G.txt")
start_time = datetime.now()
data=[]
with open("/Users/WofloW/project/test/file_500M.txt") as file:
    for line in file:
        words = line.split()
        for word in words:
            data.append(word)
            # each line in the output has exactly one word in it
data.sort()
# print data
end_time = datetime.now()
print('Duration of seq sort: {}'.format(end_time - start_time))
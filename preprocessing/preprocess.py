import csv
import sys

rf = open(sys.argv[1], 'rt')
wf = open(sys.argv[2], 'wb')
try:
    reader = csv.reader(rf)
    writer = csv.writer(wf)
    for row in reader:
	bad_flag=0
        if ((row[5] == '0') & (row[6] == '0')):
		bad_flag=1
		#print len(row)
		print "Pick up location is zero"
	elif((row[9] == '0') & (row[10] == '0')):
		bad_flag=1
		print "Drop off location is zero"
	elif((row[5] == row[9]) & (row[6] == row[10])):
		bad_flag=1 
		print "Pick up and drop off locations are same"
	elif(len(row) != 18):
		bad_flag=1
		print "row has corrupted data"
	if(bad_flag ==0):
		writer.writerow(row)
finally:
    rf.close()

import csv
import numpy as np
import matplotlib.pyplot as plt
import sys


used = []
time = []
time2 = []
memtable = []
memsize = []
count = 0

with open('d_memory.log', 'r') as csvfile:
	csv1 = csv.reader(csvfile)
	for row in csv1:
		if row[0] == 'used:':
			time.append(long(row[1]))
			used.append(int(row[2]))
			memsize.append(int(row[3]))

with open('c_memory.log', 'r') as csvfile:
	csv1 = csv.reader(csvfile)
	for row in csv1:
		if (count == 0):
			start = long(row[0])
			count = 1
		memtable.append(int(row[4]))
		time2.append(long(row[0]))

time = [float(i-start)/1000000000.0 for i in time]
time2 = [float(i-start)/1000000000.0 for i in time2]

print len(time), len(used)
plt.plot(time,used,label='Used Memory after GC')
plt.plot(time2,memtable, label='Memtable Size')
plt.plot(time,memsize, label='Max Memtable Size')
plt.ylabel('Size (MB)')
plt.xlabel('Time (Second)')
plt.xlim([time2[0],time2[-1]])
plt.title('Title')
plt.legend(loc=2)
plt.show()

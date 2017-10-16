import csv
import numpy as np
import matplotlib.pyplot as plt
import sys


free = []
used = []

with open('c_memory.log', 'r') as csvfile:
	csv1 = csv.reader(csvfile)
	for row in csv1:
		free.append(int(row[2]))
		used.append(int(row[4]))

plt.plot(range(len(free)),free)
plt.plot(range(len(used)),used)
plt.ylabel('Free Memory (MB)')
plt.xlabel('Time (0.1s)')
plt.title('MemSize = 1/8 JVM Heap')
plt.show()


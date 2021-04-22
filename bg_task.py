import time
import csv


def background_task(string):
    print('herehereherehereherehereherehereherehereherehereherehereherehereherehere')
    delay = 2
    time.sleep(delay)
    with open('task file.csv', 'w') as f:
        write = csv.writer(f, delimiter=',', lineterminator='\n')
        write.writerow(('a', 'b', 'c'))

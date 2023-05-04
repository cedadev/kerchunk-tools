import json
import matplotlib.pyplot as plt

alpha = open('original.json','r')
a = json.load(alpha)
alpha.close()

beta = open('unpacked.json','r')
b = json.load(beta)
beta.close()

diffs, simms = 0,0
init = None
count = 0

original = []
new = []

for key in a['refs'].keys():
    ameta = a['refs'][key]
    try:
        bmeta = b['refs'][key]
    except:
        bmeta = ''
    
    if ameta != bmeta:
        original.append(ameta[1])
        new.append(bmeta[1])
    count += 1


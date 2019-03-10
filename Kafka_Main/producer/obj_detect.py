import os
#os.chdir("/Users/kashish/Downloads/yolov3")
from yolov3 import yolo_opencv

#os.system("ls")
Path  = "/Users/kashish/Desktop/s1.png"
config = "/Users/kashish/darknet/cfg/yolov3.cfg"
weights = "/Users/kashish/darknet/yolov3.weights"
classes = "/Users/kashish/Downloads/yolov3/yolov3.txt"
persons  = yolo_opencv.detect_persons(Path,config,weights,classes)
print(persons)
"""
n = os.system("python3 yolo_opencv.py "+Path+config+weights+classes)
"""
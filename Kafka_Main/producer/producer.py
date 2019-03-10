import sys
import time
import cv2
from kafka import KafkaProducer
import numpy as np
import os
#os.chdir("/Users/kashish/Downloads/yolov3")
from yolov3 import yolo_opencv

config = "C:/Users/Het Dagli/Desktop/Kafka_Demo/producer/yolov3.cfg"
weights = "C:/Users/Het Dagli/Desktop/Kafka_Demo/producer/yolov3.weights"
classes = "C:/Users/Het Dagli/Desktop/Kafka_Demo/producer/yolov3/yolov3.txt"
topic = "javainuse-topic"
flag=False
flag2=0
def publish_video(video_file):
    """
    Publish given video file to a specified Kafka topic. 
    Kafka Server is expected to be running on the localhost. Not partitioned.
    
    :param video_file: path to video file <string>
    """
    # Start up producer
    producer = KafkaProducer(bootstrap_servers='localhost:9092')

    # Open file
    video = cv2.VideoCapture(video_file)
    ret,frame1 = video.read()
    while(frame1 is None):
        ret,frame1 = video.read()
    
    print('publishing video...')
    prvs = cv2.cvtColor(frame1,cv2.COLOR_RGB2GRAY)
    hsv = np.zeros_like(frame1)
    hsv[...,1] = 255
    count = 0
    prev_count = 25
    #print(video.isOpened())
    while(video.isOpened()):
        #print("Video opened")
        success, frame = video.read()
        ret, frame2 = video.read()
        #print(frame2.shape)
        if(frame2 is not None):
            next = cv2.cvtColor(frame2,cv2.COLOR_RGB2GRAY)
            curr = next
            prev = prvs
            flow = cv2.calcOpticalFlowFarneback(prvs,next, None, 0.5, 3, 15, 3, 5, 1.2, 0)


        mag, ang = cv2.cartToPolar(flow[...,0], flow[...,1])
        #print(np.sum(mag))
        if(np.sum(mag)>3*(10**3)):
            count += 1
            if(count >= 12):
                flag=True
                #print(frame2.shape)
                #cv2.imshow('frame2', frame2)

            else :
                x = np.zeros((frame2.shape))
                #print(frame2.shape)
                #cv2.imshow("frame2",x)
                flag=False
        else :
            if count > 0:
                count = count-1

            #x = np.zeros((frame2.shape))
            #cv2.imshow("frame2", x)


        hsv[...,0] = ang*180/np.pi/2
        hsv[...,2] = cv2.normalize(mag,None,0,255,cv2.NORM_MINMAX)
        rgb = cv2.cvtColor(hsv,cv2.COLOR_HSV2BGR)




        k = cv2.waitKey(30) & 0xff
        if k == 27:
            break
        elif k == ord('s'):
            #cv2.imwrite('opticalfb.png',frame2)
            cv2.imwrite('opticalhsv.png',rgb)
            #print("")
        if(next is not None):
            prvs = next
        # Ensure file was read successfully
        if not success:
            print("bad read!")
            break
        #print(prev_count)
        if(flag==True):
            if(prev_count>=25):
                persons=yolo_opencv.detect_persons(frame2,config,weights,classes)
                #print(persons)
                if(persons>=4):
                    flag2=1
                    #img_name = "frame"+str(persons)+".png"
                    #cv2.imsave(img_name,frame2)
                prev_count = 0
            else :
                prev_count = prev_count+1
                
            
                   
        else:
            flag2=0

        # Convert image to png
        
        ret, buffer = cv2.imencode('.jpg', frame2)
        # Convert to bytes and send to kafka
        message=buffer.tobytes()
        message=message+bytes([flag2])
        #print(message[-1:])
        producer.send(topic, message)
        #producer.send(topic,flag)

        time.sleep(0.2)
    video.release()
    print('publish complete')

def publish_camera():
    """
    Publish camera video stream to specified Kafka topic.
    Kafka Server is expected to be running on the localhost. Not partitioned.
    """

    # Start up producer
    producer = KafkaProducer(bootstrap_servers='localhost:9092')

    
    camera = cv2.VideoCapture(0)
    try:
        while(True):
            success, frame = camera.read()
        
            ret, buffer = cv2.imencode('.jpg', frame)
            producer.send(topic, buffer.tobytes())
            
            # Choppier stream, reduced load on processor
            time.sleep(0.2)

    except:
        print("\nExiting.")
        sys.exit(1)

    
    camera.release()


if __name__ == '__main__':
    """
    Producer will publish to Kafka Server a video file given as a system arg. 
    Otherwise it will default by streaming webcam feed.
    """
    if(len(sys.argv) > 1):
        video_path = sys.argv[1]
        publish_video('C:/Users/Het Dagli/Desktop/Kafka_Demo/videos/crime1.mp4')
    else:
        print("publishing feed!")
        publish_camera()
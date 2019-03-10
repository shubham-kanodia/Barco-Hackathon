import datetime
from flask import Flask, Response, render_template, redirect, jsonify
from kafka import KafkaConsumer

# Fire up the Kafka Consumer
topic = "javainuse-topic"
topic2= "javainuse-topic2"
consumer = KafkaConsumer(
    topic, 
    bootstrap_servers=['localhost:9092'])

consumer2 = KafkaConsumer(
    topic2, 
    bootstrap_servers=['localhost:9092'])

# Set the consumer in a Flask App
app = Flask(__name__)

crime = 0
Flag=False
@app.route('/')
def index():
    #print(crime)
    return render_template('index.html',result = 0)

# @app.route('/text_feed', methods=['GET'])
# def text_feed():
#     data = {'name': 'nabin khadka'}
#     return jsonify(data)
    

@app.route('/video_feed', methods=['GET'])
def video_feed():
    """
    This is the heart of our video display. Notice we set the mimetype to 
    multipart/x-mixed-replace. This tells Flask to replace any old images with 
    new values streaming through the pipeline.
    """
    return Response(
        get_video_stream(), 
        mimetype='multipart/x-mixed-replace; boundary=frame')
@app.route('/video_feed2', methods=['GET'])
def video_feed2():
    """
    This is the heart of our video display. Notice we set the mimetype to 
    multipart/x-mixed-replace. This tells Flask to replace any old images with 
    new values streaming through the pipeline.
    """
    return Response(
        get_video_stream2(), 
        mimetype='multipart/x-mixed-replace; boundary=frame')


def get_video_stream():
    """
    Here is where we recieve streamed images from the Kafka Server and convert 
    them to a Flask-readable format.
    """
    for msg in consumer:
        #arr=msg.value.split(bytes(["\\"]))
        val=int.from_bytes(msg.value[-1:], byteorder='big', signed=False)
        #global fnal_val
        if(val==1):
            print("Anomaly Detected")
            #print("TimeStamp"+val.timestamp)
        #fnal_val = val
       # yield(val)
        #yield (msg.value[-1:])
        yield (b'--frame\r\n'
               b'Content-Type: image/jpg\r\n\r\n' + msg.value + b'\r\n\r\n')
def get_video_stream2():
    """
    Here is where we recieve streamed images from the Kafka Server and convert 
    them to a Flask-readable format.
    """
    for msg in consumer2:
        #val=int.from_bytes(msg.value[-1:], byteorder='big', signed=False)
        #global fnal_val
        #if(val==1):
            #print("Anomaly Detected")
        yield (b'--frame\r\n'
               b'Content-Type: image/jpg\r\n\r\n' + msg.value + b'\r\n\r\n')

if __name__ == "__main__":
    app.run()


import logging
from datetime import datetime,timedelta
logging.basicConfig(filename=datetime.now().strftime('%d_%m_%Y.log'),
                    filemode='a',
                    format='%(asctime)s:%(msecs)d-%(name)s-%(levelname)s-%(message)s',
                    datefmt='%d-%m-%Y %H:%M:%S',level=logging.DEBUG)

try:
    import pkg_resources.py2_warn
    from flask import Flask, render_template, Response,request,jsonify
    import cv2
    from flask_cors import CORS, cross_origin
    import xml.etree.ElementTree as ET
    import time
    import threading,queue

    time.sleep(5)
    tree = ET.parse('Live_stream.xml')
    root = tree.getroot()
    config_data = []
    for elem in root:
        for subelem in elem:
            config_data.append(subelem.text)

    conf_user_name = config_data[0]
    conf_password = config_data[1]
    conf_ip = config_data[2]
    port = int(config_data[3])
    camera = None
except Exception as e:
    logging.error(e, exc_info=True)
    
    
class VideoCapture:
    def __init__(self, name):
        self.cap = cv2.VideoCapture(name)
        #self.cap.set(cv2.CAP_PROP_FPS,10)
        #self.cap.set(cv2.CAP_PROP_FPS, 1);
        self.q = queue.Queue()
        t = threading.Thread(target=self._reader)
        self.stop_threads=False
        t.daemon = False
        t.start()

    def _reader(self):
        while True:
            ret, frame = self.cap.read()
            if self.stop_threads: 
                break
            if not ret:
                break
            if not self.q.empty():
                try:
                    self.q.get_nowait() 
                except Queue.Empty:
                    pass
            self.q.put(frame)

    def read(self):
        return self.q.get()
        
    def __del__(self):
        #self.stop_threads=False
        #t.start()
        cv2.destroyAllWindows()
        print("deleted")
    


app = Flask(__name__)
cors = CORS(app)
ip = None

#camera = cv2.VideoCapture(0)  # use 0 for web camera
#  for cctv camera use rtsp://username:password@ip_address:554/user=username_password='password'_channel=channel_number_stream=0.sdp' instead of camera

def gen_frames():  # generate frame by frame from camera
    global ip
    global camera
    global conf_user_name
    global conf_password
    print(ip)    
    #camera = VideoCapture("rtsp://{}:{}@{}".format(conf_user_name,conf_password,ip))
    while True:
        # Capture frame-by-frame
        #camera = cv2.VideoCapture("rtsp://admin:admin@%s"%ip)
        #camera = cv2.VideoCapture("rtsp://{}:{}@{}".format(conf_user_name,conf_password,ip))
        try:
            frame = camera.read() 
        except:
            pass
        if len(frame.shape)==3:
            print(frame.shape)
                # read the camera frame
          #      if not success:
         #           break
         #       else:
                    #frame = cv2.resize(frame, (100, 50))
            ret,buffer = cv2.imencode('.jpg', frame)
            #print(len(frame))
            frame = buffer.tobytes()
                   
            yield (b'--frame\r\n'
                   b'Content-Type: image/jpeg\r\n\r\n' + frame + b'\r\n')  # concat frame one by one and show result


@app.route('/video_feed')
def video_feed():
    """Video streaming route. Put this in the src attribute of an img tag."""
    return Response(gen_frames(),
                    mimetype='multipart/x-mixed-replace; boundary=frame')


#@app.route('/data')
#def data():
 #   global ip
 #   # here we want to get the value of user (i.e. ?user=some-value)
 #   ip = request.args.get('IP')
 #   print(ip)
    
    
#@app.route('/', methods = ['POST'])
#def postJsonHandler():
#    global ip
 #   #label = "White"
 #   #print (request.is_json)
 #   content = request.get_json()
  #  ip = content["IP"]
    #frame = base64.b64decode(content["name"])
    


@app.route('/')
#def index():
#    """Video streaming home page."""
#    return render_template('index.html')
   
def index():
    global ip
    global camera
    global conf_ip
    global conf_user_name
    global conf_password
    ip = request.args.get('IP')
    #if camera is not None:
    if ip =="CLOSE":
        try:
            camera.stop_threads=True
            camera.cap.release
            del camera
        except:
            pass
        return jsonify({'Status':0})
    #time.sleep(10)
    # here we want to get the value of user (i.e. ?user=some-value)
    
    camera = VideoCapture("rtsp://{}:{}@{}".format(conf_user_name,conf_password,ip))
    #camera = VideoCapture(0)
    #if ip=="CLOSE":
    #    del camera
    #    camera = None
    #    return None
    print(ip)
    return render_template('index.html')

if __name__ == '__main__':
    try:
        app.run(host='%s'%conf_ip,port= '%d'%port,debug = False)
    except Exception as e:
        logging.error(e, exc_info=True)
    
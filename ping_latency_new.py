import subprocess
def pings(hostname):
    batcmd="ping -n 1 " + hostname
    try:
        result = subprocess.check_output(batcmd, shell=True)        
        if b'unreachable' in result or b'timed out' in result:
            return 0,-1
        else:
            try:
                lat = str(result)
                lat = lat.split("=")[2]
                lat = lat.split(" ")[0]
                lat = int(lat.split("m")[0])
                return 1,lat
            except:
                return 1,-1
    except Exception as e:
        #print(e)
        return 0,-1
        
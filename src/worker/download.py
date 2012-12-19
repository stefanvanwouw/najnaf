import urllib2
import shutil
import urlparse
import os
import subprocess
import sys
from PIL import Image

IMAGE_DIR = 'images/'

# NOTE: def download(url, fileName=None) is taken from:
# http://stackoverflow.com/questions/862173/how-to-download-a-file-using-python-in-a-smarter-way
def download(url, message_id, fileName=None):
    def getFileName(url,openUrl):

        if not os.path.exists(IMAGE_DIR):
            os.makedirs(IMAGE_DIR)
        if 'Content-Disposition' in openUrl.info():
            # If the response has Content-Disposition, try to get filename from it
            cd = dict(map(
                lambda x: x.strip().split('=') if '=' in x else (x.strip(),''),
                openUrl.info()['Content-Disposition'].split(';')))
            if 'filename' in cd:
                filename = cd['filename'].strip("\"'")
                if filename: return filename
        # if no filename was found above, parse it out of the final URL.
        return os.path.basename(urlparse.urlsplit(openUrl.url)[2])

    r = urllib2.urlopen(urllib2.Request(url))
    fileName = fileName or getFileName(url,r)
    if not os.path.exists(IMAGE_DIR + fileName):
        r = urllib2.urlopen(urllib2.Request(url))
        try:
            with open(IMAGE_DIR + fileName, 'wb') as f:
                shutil.copyfileobj(r, f)
        finally:
            r.close()

    downloadedFile = message_id + '_' + fileName
    shutil.copyfile(IMAGE_DIR + fileName, IMAGE_DIR + downloadedFile)
    
    return downloadedFile

def resize(fileName, size, message_id):
    im = Image.open(IMAGE_DIR + fileName)
    im.thumbnail(size, Image.ANTIALIAS)
    dir = IMAGE_DIR + message_id + '/'

    # Place the results in the same directory (used by watermark)
    if not os.path.exists(dir):
        os.makedirs(dir)
    newFileName = dir + fileName.partition('.')[0] + "_" + str(size[0]) + "x" + str(size[1]) + ".png"
    im.save(newFileName, "PNG")
    print 'resize dir = ' + dir

    return dir

def downloadAndResize(url, message_id):
    fileName = download(url, message_id)
    sizes = [(127, 128), (512, 512), (1024, 1024)]
    for size in sizes:
        fileDir = resize(fileName, size, message_id)

    watermark(fileDir)
    
    # next: remove the result (clean up!)
    shutil.rmtree(fileDir)
    os.remove(IMAGE_DIR + fileName)

def watermark(fileDir):
    # Watermark all images in fileDir - all resized images
    subprocess.call([sys.executable, 'in4392/src/worker/LB_waterMarker_v1.0.py', '-p 3',
        fileDir])

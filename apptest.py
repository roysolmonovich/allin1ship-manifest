
import os
import sys
#sys.path.insert(0, '/home/<username>/public_html/cgi-bin/myenv/lib/python2.7/site-packages')
from flask import Flask, render_template
app = Flask(__name__)

@app.route('/')
@app.route('/home')
def home():
    return render_template('index.html')

if __name__ == '__main__':
    app.debug = True
    app.run(port=5000)
	
#    app.run(host='162.241.219.134')
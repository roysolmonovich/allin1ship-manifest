BELOW IS A DOCUMENTATION OF THE CODE AND DEPENDENT FILES USED
FOR THE MAINTENANCE AND DEVELOPMENT OF THE BILLING SYSTEM
----------------------------------------------------------------------
COMMAND-LINE INTERFACE

A command-line interface can be run to perform some common tasks.
Before running, make sure you have a 3.9 version of Python installed
from https://www.python.org/downloads/.
Then, use the command-line to navigate to the current directory,
and enter the following command to install dependencies:
"pip install -r interface_req.txt"
After everything installs successfully, run the interface program
by entering the following:
"python interface.py"

----------------------------------------------------------------------
ZENVENTORY

To run zenventory once between specified dates, use the interface.
To run zenventory schedule, run the following command in a
command-prompt from current directory:
"python zenventory.py schedule"

----------------------------------------------------------------------
FILES

The main files and directories are the following:

\interface.py
	an interface that allows to create, read, update, and delete
	customers and customer rates, upload credit memos, run and
	schedule Zenventory API calls, run invoices, schedule FTP
	scans, and create and retrieve backups

\customers.json
	the customer info records

\app.py
	the "production" manifest app file (Heroku)

\"apptst_h - Copy.py"
	the "development" manifest app file (local) used for testing

\app_lib.py
	A slim version of the main library for Heroku manifest app

\Procfile
	Heroku procfile

\procfile alt.txt
	content should replace Procfile to add async worker

\requirements.txt
	Python requirements for Heroku

\redis_cred.py
	Redis dB URI - should be utilized to support async calls to
	manifest API

\blocklist.py
	A non-persistent set of revoked login tokens for manifest API
	used when users log out of app

\zenventory tracking to service&charge.pkl
	Tracking #: [service, charges] hash table

\main_invoice.py
	the bill generator

\zenventory.py
	a program that posts the shipping costs of a client to
	Zenventory API

\zen_lib.py
	A library for the zenventory program

\c.py
	MySQL, MongoDB (free version), Redis (free version) URIs

\db.py
	SQLAlchemy initializer for manifest API

\flag_map.pkl
	Account flag: description mapper for invoices

\flag_map.csv
	Account flag and description table

\Zenventory API\
	Directory of "Charge Not Found" and "Post Data" csv files from
	the Zenventory API shipments:
	Each file name contains
	<date from> - <date to> - <datetime of processing>

\customers_backup\
	A directory for customer record backups

\temp\
	Account not found records from individual invoice runs are
	written into csv files in this directory

\Manifests\
	A directory that stores the manifests and contains the
	format.json manifest format file

\Customer Profit by Week\Customer Profit by Week.xlsx
	Account profit by week pivot table

\invoices\
	invoice directory

\invoices\invoices.json
\invoices\shipping_variance.json
\invoices\marked_up.json
\invoices\overlabeled.json
	invoices and dependent file logs, used to check if weekly
	files were received before bill generator can run

\models\manifest.py
\models\user.py
	manifest RESTful API models

\resources\manifest.py
\resources\user.py
	manifest RESTful API resources

\schemas\manifest.py
	form and json input validators for manifest API requests

\venv\
	Python virtual environment for manifest API
	to activate, run "venv\Scripts\activate.bat" (Windows) from
	terminal. Used as a separate environment to install
	only API libraries.

\dependencies\charges_by_zone\carrier_charges111.pkl
	charge mapper: 
	  carrier\tier:
	    location (domestic\international):
	      date:
	        service:
	          zone:
	            weight: charge
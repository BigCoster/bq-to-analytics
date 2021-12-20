import os
basedir = os.path.abspath(os.path.dirname(__file__))

# google credentials
GOOGLE_CRED_PATH = os.path.join(basedir, 'client_secret.json')
GOOGLE_PROJ_ID = 'vocal-framework-241518'

LOG_PATH = os.path.join(basedir, 'app.log')
# LOG_PATH = '/var/log/uwsgi/bq2ga.log'

PROJ = [
    {'view': '172487921', 'site': 'abc-decor.com', 'tracker': 'UA-116546585-1'},
    {'view': '112798836', 'site': 'art-oboi.com.ua', 'tracker': 'UA-70903943-1'},
    {'view': '113470694', 'site': 'walldeco.ua', 'tracker': 'UA-71429340-1'},
    {'view': '91697505', 'site': 'art-holst.com.ua', 'tracker': 'UA-25405474-7'},
    {'view': '91906521', 'site': 'klv-oboi.ru', 'tracker': 'UA-31244245-1'},
    {'view': '166286564', 'site': 'fotooboi.biz', 'tracker': 'UA-111167426-1'},
    {'view': '98144201', 'site': 'uwalls.pl', 'tracker': 'UA-59891465-1'},
    {'view': '189107448', 'site': 'uwalls.de', 'tracker': 'UA-133658094-1'},
    {'view': '111468487', 'site': 'uwalls.com.ua', 'tracker': 'UA-69910846-1'},
    {'view': '195593263', 'site': 'uwalls.fr', 'tracker': 'UA-140710427-1'},
    {'view': '195619540', 'site': 'uwalls.it', 'tracker': 'UA-140675130-1'},
    {'view': '88047046', 'site': 'uwalls.com', 'tracker': 'UA-52445195-1'},
    {'view': '206177161', 'site': 'art-gifts.com.ua', 'tracker': 'UA-152558641-1'},
    {'view': '216615254', 'site': 'picwalls.ru', 'tracker': 'UA-164487392-1'}
]

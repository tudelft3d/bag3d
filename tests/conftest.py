import pytest
import os.path

from bag3d.config import db

#-------------------------------------------------------------------- testing DB
@pytest.fixture(scope="session")
def batch3dfier_db(request):
    dbs = db.db(dbname='batch3dfier_db', host='localhost', port='5432',
                user='batch3dfier')

    def disconnect():
        dbs.close()
    request.addfinalizer(disconnect)

    return(dbs)


#-------------------------------------------------------------- data & folders
@pytest.fixture(scope="session")
def schema():
    root =  os.getcwd()
    return os.path.join(root, "bag3d_config_schema.yml")

@pytest.fixture(scope="session")
def config():
    root =  os.getcwd()
    return os.path.join(root, "bag3d_config.yml")
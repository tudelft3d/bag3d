import pytest
import os.path


#-------------------------------------------------------------- data & folders
@pytest.fixture(scope="session")
def schema():
    root =  os.getcwd()
    return os.path.join(root, "bag3d_config_schema.yml")

@pytest.fixture(scope="session")
def config():
    root =  os.getcwd()
    return os.path.join(root, "bag3d_config.yml")
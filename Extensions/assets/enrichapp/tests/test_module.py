import os 
import sys 
import json 
import pytest 
import requests

from enrichapp.discover.monitor.backends import MLFlowBackend

thisdir = os.path.dirname(__file__)
dataroot = os.path.join(thisdir, 'fixtures', 'mlflow')    
mapping = {
    "experiments/list": json.load(open(os.path.join(dataroot,'list.json'))),
    "experiments/get": json.load(open(os.path.join(dataroot,'get-0.json'))),
    "runs/get": json.load(open(os.path.join(dataroot,'run-get-0.json'))),
}

def initialize_mock(m):

    import requests_mock
    
    urlroot = "http://dummy/api/2.0/preview/mlflow"

    fullurls = [] 
    for suffix, data in mapping.items():
        fullurl = urlroot + "/" + suffix
        if suffix == 'experiments/get':
            fullurl = fullurl + "?experiment_id={}".format(data['experiment']['experiment_id'])
        elif suffix == 'runs/get':
            fullurl = fullurl + "?run_uuid={}".format(data['run']['info']['run_uuid'])

        fullurls.append(fullurl) 
        m.register_uri('GET',
                       fullurl, 
                       json=data,
                       status_code=200)

    # => Catch all for everything else
    def match_request_text1(request):
        return request.url not in fullurls 
    
    m.register_uri(requests_mock.ANY, requests_mock.ANY,
                   text="Error",
                   additional_matcher=match_request_text1,
                   status_code=404) 

@pytest.fixture()
def mlflow_backend(): 
    creds = {
        "root": "http://dummy/api/2.0/preview/mlflow",
        "user": "A",
        "password": "B"
    }

    mlflow = MLFlowBackend(creds)
    return mlflow

def test_experiments(requests_mock, mlflow_backend):

    initialize_mock(requests_mock) 

    # Get experiments
    expts = mlflow_backend.get_experiments() 
    
    assert len(expts) == 1 


def test_runs(requests_mock, mlflow_backend):

    initialize_mock(requests_mock) 

    experiments = mlflow_backend.get_experiments()

    first_exp = experiments['experiments'][0]['experiment_id']
    runs = mlflow_backend.get_runs(experiment_id=first_exp)

    # Should see 30 runs 
    assert len(runs) == 30
    

def test_invalid_experiment(requests_mock, mlflow_backend):

    initialize_mock(requests_mock) 

    with pytest.raises(Exception):
        mlflow_backend.get_experiment_detail(experiment_id=23)
    
def test_invalid_run(requests_mock, mlflow_backend):

    initialize_mock(requests_mock) 

    with pytest.raises(Exception):
        mlflow_backend.get_experiment_detail(run_uuid=23)

    
        

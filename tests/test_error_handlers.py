from testutils import app  # noqa: F401


def test_not_found(client):
    '''Test 404 error handler'''
    results = client.get('/totally/made/up')
    assert results.status_code == 404
    assert results.json == {'error': 'Not found'}


def test_method_not_allowed(client):
    '''Test 405 error handler'''
    results = client.get('/api/v1.0/search')
    assert results.status_code == 405
    assert results.json == {'error': 'Method not allowed'}


def test_internal_server_error(client):
    '''Test 500 error handler'''
    results = client.get('/api/v1.0/stats')
    assert results.status_code == 500
    assert results.json == {'error': 'Internal server error'}

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

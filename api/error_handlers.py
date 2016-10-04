'''JSONified error handlers'''
from flask import make_response, jsonify
from . import app
from .errors import TooManyResults


@app.errorhandler(400)
def bad_req(error):
    return make_response(jsonify({'error': 'Bad request'}), 400)


@app.errorhandler(403)
def forbidden(error):
    return make_response(jsonify({'error': 'Forbidden'}), 403)


@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Not found'}), 404)


@app.errorhandler(405)
def method_not_allowed(error):
    return make_response(jsonify({'error': 'Method not allowed'}), 405)


@app.errorhandler(500)
def internal_server_error(error):
    return make_response(jsonify({'error': 'Internal server error'}), 500)


@app.errorhandler(TooManyResults)
def too_many_results(error):
    return make_response(jsonify(error.to_dict()), 400)

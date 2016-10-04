'''Custom errors for the REST API'''


class TooManyResults(Exception):
    status_code = 400

    def __init__(self, message, status_code=None, payload=None):
        Exception.__init__(self)
        self.message = message
        if status_code is not None:
            self.status_code = status_code
        self.payload = payload


    def to_dict(self):
        '''Convert exception to dict for JSON rendering'''
        ret = dict(self.payload or ())
        ret['error'] = self.message
        return ret

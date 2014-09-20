__author__ = 'bwillard'

import logging

from endless import endless_server
app = endless_server.app


if __name__ == '__main__':

    logging.basicConfig(level=logging.DEBUG)

    port = 5000

    app.config.update({
        'CASSANDRA_NODES': ['localhost',]
    })

    app.run(debug=True, port=port)

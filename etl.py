import bonobo
import time
from extractor import Extractor

def extract():
    extractor = Extractor()
    for row in extractor.generate():
        yield row

def transform(*args):
    yield { 'year': args[2], 'import': args[3], 'export': args[5] }

def load(*args):
    line = args[0]
    print("Year {}: Import {} - Export {}".format(line['year'], line['import'], line['export']))

def get_graph(**options):
    graph = bonobo.Graph()

    graph.add_chain(
        extract,
        bonobo.Limit(10),
        #bonobo.PrettyPrinter(),
        transform,
        load,
    )

    return graph


def get_services(**options):
    return {}

# The __main__ block actually execute the graph.
if __name__ == '__main__':
    parser = bonobo.get_argument_parser()
    with bonobo.parse_args(parser) as options:
        bonobo.run(
            get_graph(**options),
            services=get_services(**options)
        )

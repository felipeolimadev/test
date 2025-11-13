import socket

HOST_PORT = 3000

ACTION_PREFIX = "<kg-provider-test>"
ENCODING = "utf-8"
BUFFER_SIZE = 1024


ACTIONS = [
    ["clear-all", "clear all data in server"],
    ["create-datastore", "create the datastore"],
    ["delete-datastore", "delete the datastore"],
    ["import-prefixes", "import rules/facts and prefixes"],
    ["import-schemas", "import rules/facts"],
    ["import-rules", "import rules/facts"],
    ["import-axioms", "run TBoxReasoning"],
    ["insert", "import rules/facts"],
    ["delete", "delete rules/facts"],
    ["query", "run the query"],
    ["get-triples-count", "get the number of triples"],
]


class KnowledgeProviderDebugClient:
    def __init__(self):
        self.s = socket.socket(type=socket.SOCK_STREAM)

    class StringInput:
        def __init__(self, text):
            self.text = text.strip() + "\n"

        def send(self, client):
            client._send(self.text)

    class FileInput:
        def __init__(self, path):
            self.path = path

        def send(self, client):
            client._send_file(self.path)

    def open(self):
        self.s.connect(("localhost", HOST_PORT))

    def close(self):
        self.s.close()

    def debug_begin(self):
        self._send(f"{ACTION_PREFIX} debug begin\n")

    def debug_end(self):
        self._send(f"{ACTION_PREFIX} debug end\n")

    def clear_all(self):
        self._send_begin("clear-all")
        self._receive()

    def create_datastore(self, datastore: str):
        self._send_begin("create-datastore", datastore)
        self._receive()

    def delete_datastore(self, datastore: str):
        self._send_begin("delete-datastore", datastore)
        self._receive()

    def get_triples_count(self, datastore: str):
        self._send_begin("get-triples-count", datastore)
        self._receive()

    def import_prefixes(self, input, datastore: str):
        self._send_begin("import-prefixes", datastore)
        input.send(self)
        self._shutdown_output()
        self._receive()

    def import_schemas(self, input, datastore: str):
        self._send_begin("import-schemas", datastore)
        input.send(self)
        self._shutdown_output()
        self._receive()

    def import_rules(self, input, datastore: str):
        self._send_begin("import-rules", datastore)
        input.send(self)
        self._shutdown_output()
        self._receive()

    def import_axioms(self, datastore: str):
        self._send_begin("import-axioms", datastore)
        self._receive()

    def insert(self, input, datastore: str):
        self._send_begin("insert", datastore)
        input.send(self)
        self._shutdown_output()
        self._receive()

    def delete(self, input, datastore: str):
        self._send_begin("delete", datastore)
        input.send(self)
        self._shutdown_output()
        self._receive()

    def query(self, input, datastore: str):
        self._send_begin("query", datastore)
        input.send(self)
        self._shutdown_output()
        self._receive()

    def _send_begin(self, action: str, datastore: str = ""):
        self._send(f"{ACTION_PREFIX} {action} {datastore}\n")

    def _send(self, text: str):
        self.s.send(bytes(text, ENCODING))
        self._receive_if_exists()

    def _send_file(self, path: str):
        with open(path, "rb") as f:
            self.s.sendfile(f)

    def _receive_if_exists(self):
        try:
            while True:
                line = self.s.recv(BUFFER_SIZE, socket.MSG_DONTWAIT).decode(ENCODING)
                if line:
                    print(line, end="")
                else:
                    break
        except:
            return

    def _shutdown_output(self):
        self._send(f"{ACTION_PREFIX} input end\n")

    def _receive(self):
        while True:
            line = self.s.recv(BUFFER_SIZE).decode(ENCODING)
            if line == "":
                break
            print(line, end="")


def run(args, client):
    if args.debug:
        client.debug_begin()
    else:
        client.debug_end()

    if args.action == "clear-all":
        client.clear_all()
        return

    if args.action == "create-datastore":
        client.create_datastore(args.datastore)
        return
    elif args.action == "delete-datastore":
        client.delete_datastore(args.datastore)
        return
    elif args.action == "get-triples-count":
        client.get_triples_count(args.datastore)
        return

    if args.input_file:
        input = KnowledgeProviderDebugClient.FileInput(args.input_file)
    elif args.input_string:
        input = KnowledgeProviderDebugClient.StringInput(args.input_string)
    else:
        raise NotImplementedError("Invalid input type", args)

    if args.action == "import-prefixes":
        client.import_prefixes(input, args.datastore)
        return
    elif args.action == "import-schemas":
        client.import_schemas(input, args.datastore)
        return
    elif args.action == "import-rules":
        client.import_rules(input, args.datastore)
        return
    elif args.action == "import-axioms":
        client.import_axioms(input, args.datastore)
        return
    elif args.action == "insert":
        client.insert(input, args.datastore)
        return
    elif args.action == "delete":
        client.delete(input, args.datastore)
        return
    elif args.action == "query":
        client.query(input, args.datastore)
        return
    else:
        raise NotImplementedError("Invalid action type", args)


class bcolors:
    HEADER = "\033[95m"
    OKBLUE = "\033[94m"
    OKCYAN = "\033[96m"
    OKGREEN = "\033[92m"
    WARNING = "\033[93m"
    FAIL = "\033[91m"
    ENDC = "\033[0m"
    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"


def action_types():
    return [i[0] for i in ACTIONS]


def action_descriptions():
    max_length = 0
    max_length = len(max(action_types(), key=lambda a: len(a)))

    return "available actions:\n" + "\n".join(
        [f"{i[0].rjust(max_length+4, ' ')} : {i[1]}" for i in ACTIONS]
    )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter, epilog=action_descriptions()
    )
    parser.add_argument(
        "--debug",
        default=False,
        action="store_true",
        help="Activate debug info",
    )
    parser.add_argument(
        "--action",
        required=True,
        choices=action_types(),
        help="See below for supported action types.",
        metavar="ACTION",
    )
    parser.add_argument(
        "--datastore",
        type=str,
        default="",
        help="Name of target datastore. Default datastore will be used if not given.",
    )
    parser.add_argument(
        "--input-file",
        type=str,
        default=None,
        help="Path to the input file.",
    )
    parser.add_argument(
        "--input-string",
        type=str,
        default=None,
        help="Input string to be sent.",
    )

    args = parser.parse_args()

    try:
        client = KnowledgeProviderDebugClient()
        client.open()
        run(args, client)
        client.close()
    except ConnectionRefusedError as e:
        print(e)
        print(
            f"\t{bcolors.FAIL}[ERROR]{bcolors.ENDC} Please check port {bcolors.OKGREEN}{HOST_PORT}{bcolors.ENDC} is available:"
        )
        print(
            f"\t$ {bcolors.OKGREEN}adb forward tcp:{HOST_PORT} localabstract:kg.provider.test.server{bcolors.ENDC}"
        )
        print(f"\t$ {bcolors.OKGREEN}adb forward --list{bcolors.ENDC}")
    except Exception as e:
        import traceback

        traceback.print_exception(type(e), e, e.__traceback__)
        client.close()

class BufferFullError(Exception):
    def __init__(self, *args):
        if args:
            self.message = args[0]
        else:
            self.message = 'Buffer is full.'

    def __str__(self):
        return 'Error: ' + self.message

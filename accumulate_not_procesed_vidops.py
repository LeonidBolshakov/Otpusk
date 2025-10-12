import logging


# noinspection SpellCheckingInspection
class AccumulateNotProcessedVidops(logging.Handler):
    def __init__(self):
        super().__init__()
        self.not_processing_vidop = "not_processing_vidop"
        self.accumulate = set()

    def emit(self, record):
        message = record.getMessage()
        if self.not_processing_vidop in message:
            vidop = message.split(self.not_processing_vidop)[1]
            self.accumulate.add(vidop)

    def output_accumulate(self):
        return self.accumulate

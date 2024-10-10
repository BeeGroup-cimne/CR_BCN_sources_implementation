from sources import SourcePlugin
from sources.Inspire.gather import gather
from sources.Inspire.harmonizer import harmonize_command_line
from sources.Endesa.harmonizer.endesa import harmonize_endesa


class Plugin(SourcePlugin):
    source_name = "endesa"

    def gather(self, arguments):
        gather(arguments, settings=self.settings, config=self.config)

    def harmonizer_command_line(self, arguments):
        harmonize_command_line(arguments, config=self.config, settings=self.settings)

    def get_mapper(self, message):
        if message["collection_type"] == 'devices':
            return harmonize_data_device

    def get_kwargs(self, message):
        return {
            "namespace": message['namespace'],
            "user": message['user'],
            "config": self.config,
            "timezone": message['timezone']
        }

    def get_store_table(self, message):
        if message['collection_type'] == "devices":
            return None  # Useless info
        elif message['collection_type'] == "invoices":
            return f"raw_Endesa_ts_invoices__{message['user']}"

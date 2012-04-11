import os
from datetime import datetime
from xml.etree.ElementTree import ElementTree, Element, SubElement


def parse_time(s):
    if not s:
        return None
    return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")


def format_time(d):
    if not d:
        return ''
    return d.strftime("%Y-%m-%d %H:%M:%S")


class StatusReport(object):

    def __init__(self, schema_seq=None, replication_seq=None):
        self.schema_seq = schema_seq
        self.replication_seq = replication_seq
        self.last_replication_time = None
        self.last_finished_time = None

    def end(self):
        self.last_finished_time = datetime.now()

    def update(self, replication_seq):
        self.last_replication_time = datetime.now()
        self.replication_seq = replication_seq

    def load(self, path):
        if not os.path.exists(path):
            return
        tree = ElementTree()
        tree.parse(path)
        self.schema_seq = int(tree.find("schema_seq").text)
        self.replication_seq = int(tree.find("replication_seq").text)
        self.last_replication_time = parse_time(tree.find("last_updated").text)
        self.last_finished_time = parse_time(tree.find("last_finished").text)

    def save(self, path):
        status = Element("status")
        SubElement(status, "schema_seq").text = str(self.schema_seq or 0)
        SubElement(status, "replication_seq").text = str(self.replication_seq or 0)
        SubElement(status, "last_updated").text = format_time(self.last_replication_time)
        SubElement(status, "last_finished").text = format_time(self.last_finished_time)
        tree = ElementTree(status)
        tree.write(path, encoding="UTF-8", xml_declaration=True)



__author__ = 'nkdhny'

import luigi
import os.path
import tempfile
import fnmatch
import json

class FilesInDirectory(luigi.Task):
    dir = luigi.Parameter()
    id = luigi.Parameter()
    file_pattern = luigi.Parameter(default='*.jpg')

    def output(self):
        path = os.path.join(tempfile.gettempdir(), 'runtag-detection-source.{}.json'.format(self.id))
        return luigi.LocalTarget(path=path)

    def run(self):

        fs = [os.path.join(self.dir, f) for f in os.listdir(self.dir) if os.path.isfile(os.path.join(self.dir, f))]
        fs = filter(lambda f: fnmatch.fnmatch(f, self.file_pattern), fs)

        with self.output().open('w') as o:
            json.dump(fs, o)


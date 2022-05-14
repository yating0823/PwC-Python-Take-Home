import json
import os
import shutil
import stat
import tempfile
# import yaml
import pyarrow.parquet as pq


def atomic(write):
    def atomic_write(file, content):
        # Use the same directory as the destination file
        temp_file = tempfile.NamedTemporaryFile(
            delete=False,
            dir=os.path.dirname(file))
        try:
            # preserve file metadata if it already exists
            if os.path.exists(file):
                shutil.copy2(file, temp_file.name)
                # copy owner and group
                st = os.stat(file)
                os.chown(file, st[stat.ST_UID], st[stat.ST_GID])
            # write to the temp file
            write(temp_file.name, content)
            # change the temp file to the target file
            os.replace(temp_file.name, file)
        finally:
            if os.path.exists(temp_file.name):
                try:
                    os.unlink(temp_file.name)
                except:
                    pass

    return atomic_write


@atomic
def write(file, content):
    pq.write_table(content, file)
